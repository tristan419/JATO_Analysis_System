from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import Select, or_, select
from sqlalchemy.orm import Session

from app.db.models import CurrentPrice, MsrpSource
from app.infra import msrp_repository
from app.services.country_service import to_display_country
from app.services.msrp_workflow_service import create_scrape_batch_ingest

EVKX_CONFIDENCE_PROFILE = "evkx_reference_catalog_v1"
EVKX_EXTRACTOR_VERSION = "evkx_catalog_v1"
EVKX_SOURCE_TYPE = "reference_catalog"
EVKX_PRICE_SEMANTICS = "reference_msrp_tax_unknown"

MARKET_LABEL_ALIASES: dict[str, str] = {
    "usa": "United States",
    "us": "United States",
    "unitedstates": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", _normalize_text(value))
    return slug.strip("_") or "unknown"


def _normalize_market_name(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    alias = MARKET_LABEL_ALIASES.get(raw.lower().replace(" ", ""))
    if alias:
        return alias
    return to_display_country(raw)


def _extract_schema_car(record: dict[str, Any]) -> dict[str, Any]:
    for item in record.get("schemaOrg") or []:
        if isinstance(item, dict) and item.get("@type") == "Car":
            return item
    return {}


def _extract_breadcrumb_names(record: dict[str, Any]) -> dict[int, str]:
    for item in record.get("schemaOrg") or []:
        if not isinstance(item, dict) or item.get("@type") != "BreadcrumbList":
            continue
        names: dict[int, str] = {}
        for crumb in item.get("itemListElement") or []:
            if not isinstance(crumb, dict):
                continue
            position = crumb.get("position")
            name = str(crumb.get("name") or "").strip()
            if isinstance(position, int) and name:
                names[position] = name
        return names
    return {}


def _extract_brand_model_trim(
    record: dict[str, Any],
) -> tuple[str, str, str, str]:
    car_schema = _extract_schema_car(record)
    breadcrumbs = _extract_breadcrumb_names(record)
    full_name = str(record.get("name") or "").strip()
    brand = (
        str((car_schema.get("brand") or {}).get("name") or "").strip()
        if isinstance(car_schema.get("brand"), dict)
        else ""
    )
    if not brand:
        brand = breadcrumbs.get(3, "")
    if not brand and full_name:
        brand = full_name.split(" ", 1)[0].strip()

    model = breadcrumbs.get(4, "").strip()
    variant_name = breadcrumbs.get(5, "").strip() or full_name
    if not model and variant_name:
        candidate = variant_name
        if brand and candidate.lower().startswith(brand.lower()):
            candidate = candidate[len(brand) :].strip()
        model = candidate.split(" ", 1)[0].strip()

    trim = variant_name
    if brand and trim.lower().startswith(brand.lower()):
        trim = trim[len(brand) :].strip()
    if model and trim.lower().startswith(model.lower()):
        trim = trim[len(model) :].strip()
    trim = trim or variant_name or model

    display_name = full_name or " ".join(part for part in [brand, model, trim] if part)
    return brand, model, trim, display_name.strip()


def _select_market_price(
    record: dict[str, Any],
    target_country: str,
) -> dict[str, Any] | None:
    normalized_target = _normalize_market_name(target_country)
    for entry in record.get("pricingByMarket") or []:
        if not isinstance(entry, dict):
            continue
        if _normalize_market_name(entry.get("marketLabel")) != normalized_target:
            continue
        amount = entry.get("amount")
        currency = str(entry.get("currency") or "").strip().upper()
        if amount in (None, "") or not currency:
            continue
        return {
            "marketLabel": normalized_target,
            "rawMarketLabel": entry.get("marketLabel"),
            "priceText": entry.get("priceText"),
            "amount": float(amount),
            "currency": currency,
            "selectedBy": "pricingByMarket",
        }

    fallback_country = _normalize_market_name(record.get("pricingCountry"))
    fallback_amount = record.get("startPrice")
    fallback_currency = str(record.get("currency") or "").strip().upper()
    if (
        fallback_country == normalized_target
        and fallback_amount not in (None, "")
        and fallback_currency
    ):
        return {
            "marketLabel": normalized_target,
            "rawMarketLabel": record.get("pricingCountry"),
            "priceText": None,
            "amount": float(fallback_amount),
            "currency": fallback_currency,
            "selectedBy": "searchSummary",
        }
    return None


def _pick_spec(
    specifications: dict[str, Any],
    section: str,
    *field_names: str,
) -> str | None:
    section_payload = specifications.get(section)
    if not isinstance(section_payload, dict):
        return None
    for field_name in field_names:
        value = section_payload.get(field_name)
        if value not in (None, ""):
            return str(value)
    return None


def _extract_spec_highlights(record: dict[str, Any]) -> dict[str, Any]:
    specifications = (
        record.get("specifications")
        if isinstance(record.get("specifications"), dict)
        else {}
    )
    car_schema = _extract_schema_car(record)
    highlights = {
        "bodyType": car_schema.get("bodyType"),
        "modelYear": record.get("searchSummary", {}).get("modelYear"),
        "peakPower": _pick_spec(specifications, "Performance", "Peak power"),
        "torque": _pick_spec(
            specifications,
            "Performance",
            "Electrical torque output",
        ),
        "topSpeed": _pick_spec(specifications, "Performance", "Top speed"),
        "batteryNet": _pick_spec(
            specifications,
            "Battery & Charging",
            "Battery net",
        ),
        "batteryGross": _pick_spec(
            specifications,
            "Battery & Charging",
            "Battery gross",
        ),
        "maxDcCharging": _pick_spec(
            specifications,
            "Battery & Charging",
            "Max DC charging",
        ),
        "range": (
            _pick_spec(
                specifications,
                "Range & Consumption",
                "EPA range Learn more",
            )
            or _pick_spec(
                specifications,
                "Range & Consumption",
                "WLTP range Learn more",
            )
        ),
        "consumption": (
            _pick_spec(
                specifications,
                "Range & Consumption",
                "EPA consumption Learn more",
            )
            or _pick_spec(
                specifications,
                "Range & Consumption",
                "WLTP consumption Learn more",
            )
        ),
        "trunkCapacity": _pick_spec(
            specifications,
            "Dimensions",
            "Trunk capacity",
        ),
        "trailerWeight": _pick_spec(
            specifications,
            "Dimensions",
            "Max trailer weight braked",
        ),
        "chargeport": _pick_spec(
            specifications,
            "Chargeport",
            "Type chargeport North America",
            "Type chargeport Europe",
        ),
    }
    return {key: value for key, value in highlights.items() if value not in (None, "", [])}


def _build_confidence_rule(
    record: dict[str, Any],
    target_country: str,
    selected_market_price: dict[str, Any],
    spec_highlights: dict[str, Any],
    *,
    has_candidate_matches: bool,
) -> tuple[float, dict[str, Any]]:
    components: list[dict[str, Any]] = [
        {
            "key": "evkx-reference-catalog",
            "label": "EVKX reference catalog",
            "applied": True,
            "delta": 0.42,
            "evidence": "reference catalog source",
        },
        {
            "key": "target-market-price",
            "label": "Target market price found",
            "applied": True,
            "delta": 0.18,
            "evidence": selected_market_price.get("marketLabel"),
        },
        {
            "key": "native-price",
            "label": "Native market price",
            "applied": not bool(record.get("isConverted")),
            "delta": 0.06 if not bool(record.get("isConverted")) else 0.0,
            "evidence": record.get("currency"),
        },
        {
            "key": "specifications-present",
            "label": "Specifications captured",
            "applied": bool(spec_highlights),
            "delta": 0.1 if spec_highlights else 0.0,
            "evidence": sorted(spec_highlights.keys())[:6] if spec_highlights else [],
        },
        {
            "key": "breadcrumb-variant",
            "label": "Variant breadcrumb captured",
            "applied": bool(_extract_breadcrumb_names(record)),
            "delta": 0.07 if _extract_breadcrumb_names(record) else 0.0,
            "evidence": record.get("name"),
        },
        {
            "key": "candidate-hints",
            "label": "Current-price candidate hints",
            "applied": has_candidate_matches,
            "delta": 0.04 if has_candidate_matches else 0.0,
            "evidence": "same-country current prices" if has_candidate_matches else None,
        },
    ]

    record_pricing_country = _normalize_market_name(record.get("pricingCountry"))
    if record_pricing_country and record_pricing_country != target_country:
        components.append(
            {
                "key": "market-mismatch-penalty",
                "label": "Record default market differs",
                "applied": True,
                "delta": -0.07,
                "evidence": {
                    "recordPricingCountry": record_pricing_country,
                    "targetCountry": target_country,
                },
            }
        )

    total = max(
        0.0,
        min(
            0.99,
            sum(float(component["delta"]) for component in components),
        ),
    )
    return total, {
        "mode": EVKX_CONFIDENCE_PROFILE,
        "base": components[0]["delta"],
        "total": total,
        "components": components,
    }


def _string_similarity(left: str | None, right: str | None) -> float:
    normalized_left = _normalize_text(left)
    normalized_right = _normalize_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return SequenceMatcher(a=normalized_left, b=normalized_right).ratio()


def _build_candidate_matches(
    session: Session,
    *,
    country: str,
    brand: str,
    model: str,
    trim: str,
    powertrain: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    stmt: Select[tuple[CurrentPrice]] = (
        select(CurrentPrice)
        .where(
            CurrentPrice.country == country,
            CurrentPrice.brand == brand,
            or_(
                CurrentPrice.jato_model == model,
                CurrentPrice.official_model == model,
            ),
        )
        .order_by(CurrentPrice.updated_at_utc.desc())
        .limit(80)
    )
    current_prices = session.execute(stmt).scalars().all()
    ranked: list[dict[str, Any]] = []
    for current_price in current_prices:
        model_similarity = max(
            _string_similarity(model, current_price.jato_model),
            _string_similarity(model, current_price.official_model),
        )
        trim_similarity = max(
            _string_similarity(trim, current_price.jato_trim),
            _string_similarity(trim, current_price.official_trim),
        )
        candidate_powertrain = str(
            current_price.jato_powertrain or current_price.official_powertrain or ""
        ).strip()
        powertrain_match = bool(
            powertrain
            and candidate_powertrain
            and _normalize_text(powertrain) == _normalize_text(candidate_powertrain)
        )
        score = (
            model_similarity * 0.45
            + trim_similarity * 0.45
            + (0.1 if powertrain_match else 0.0)
        )
        if score < 0.35:
            continue
        ranked.append(
            {
                "currentPriceId": str(current_price.current_price_id),
                "jatoModel": current_price.jato_model,
                "jatoTrim": current_price.jato_trim,
                "jatoPowertrain": current_price.jato_powertrain or None,
                "officialModel": current_price.official_model,
                "officialTrim": current_price.official_trim,
                "officialEdition": current_price.official_edition,
                "officialPowertrain": current_price.official_powertrain,
                "currentMsrpValue": float(current_price.current_msrp_value),
                "currency": current_price.currency,
                "score": round(score, 4),
                "reason": {
                    "modelSimilarity": round(model_similarity, 4),
                    "trimSimilarity": round(trim_similarity, 4),
                    "powertrainMatch": powertrain_match,
                },
            }
        )
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:limit]


def _build_source_context(
    record: dict[str, Any],
    *,
    target_country: str,
    selected_market_price: dict[str, Any],
    spec_highlights: dict[str, Any],
) -> dict[str, Any]:
    car_schema = _extract_schema_car(record)
    return {
        "source": "EVKX",
        "profile": EVKX_CONFIDENCE_PROFILE,
        "evId": record.get("evId"),
        "vehicleName": record.get("name"),
        "targetCountry": target_country,
        "infoUrl": record.get("infoUrl"),
        "thumbnailUrl": record.get("thumbnailUrl"),
        "pricingCountry": _normalize_market_name(record.get("pricingCountry")),
        "selectedMarketPrice": selected_market_price,
        "pricingByMarket": record.get("pricingByMarket") or [],
        "specHighlights": spec_highlights,
        "specifications": record.get("specifications") or {},
        "searchSummary": record.get("searchSummary") or {},
        "schemaSummary": {
            "brand": (
                (car_schema.get("brand") or {}).get("name")
                if isinstance(car_schema.get("brand"), dict)
                else None
            ),
            "bodyType": car_schema.get("bodyType"),
            "vehicleModelDate": car_schema.get("vehicleModelDate"),
        },
    }


def _compute_payload_hash(
    record: dict[str, Any],
    *,
    selected_market_price: dict[str, Any],
    target_country: str,
) -> str:
    payload = {
        "evId": record.get("evId"),
        "name": record.get("name"),
        "targetCountry": target_country,
        "selectedMarketPrice": selected_market_price,
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_source_code(country: str, brand: str) -> str:
    return f"evkx_{_slugify(country)}_{_slugify(brand)}_catalog"


def _ensure_source(
    session: Session,
    *,
    country: str,
    brand: str,
    raw_pricing_country: str | None,
) -> MsrpSource:
    source_code = _build_source_code(country, brand)
    existing = msrp_repository.get_source_by_code(session, source_code)
    if existing is not None:
        return existing

    pricing_country = raw_pricing_country or country
    source = MsrpSource(
        source_code=source_code,
        country=country,
        brand=brand,
        source_url=(
            "https://evkx.net/evsearch/?page=1&pageSize=20&sortOrder=Name"
            f"&availabilityFilter=current&pricingCountry={quote_plus(pricing_country)}"
        ),
        source_type=EVKX_SOURCE_TYPE,
        tier=2,
        extractor_name="evkx_catalog",
        extractor_version=EVKX_EXTRACTOR_VERSION,
        price_semantics=EVKX_PRICE_SEMANTICS,
        requires_location=False,
        enabled=True,
        notes="Imported from EVKX catalog JSON",
    )
    msrp_repository.add_source(session, source)
    session.flush()
    return source


def _load_catalog(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("EVKX catalog must be a JSON object")
    return payload


def build_evkx_scrape_batch_payload(
    session: Session,
    catalog_path: str | Path,
    *,
    target_country: str | None = None,
    batch_code: str | None = None,
    observed_at_utc: datetime | None = None,
) -> dict[str, Any]:
    path = Path(catalog_path)
    catalog = _load_catalog(path)
    metadata = catalog.get("metadata") if isinstance(catalog.get("metadata"), dict) else {}
    requested_country = target_country or metadata.get("pricingCountry")
    normalized_country = _normalize_market_name(str(requested_country or ""))
    if not normalized_country:
        raise ValueError("target_country is required for EVKX import")

    observed_at = observed_at_utc or _utc_now()
    scope_brands: list[str] = []
    observations: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for record in catalog.get("records") or []:
        if not isinstance(record, dict):
            continue
        selected_market_price = _select_market_price(record, normalized_country)
        if selected_market_price is None:
            skipped.append(
                {
                    "evId": record.get("evId"),
                    "name": record.get("name"),
                    "reason": "target_market_price_missing",
                }
            )
            continue

        brand, model, trim, display_name = _extract_brand_model_trim(record)
        if not brand or not model or not trim:
            skipped.append(
                {
                    "evId": record.get("evId"),
                    "name": record.get("name"),
                    "reason": "variant_identity_incomplete",
                }
            )
            continue

        scope_brands.append(brand)
        source = _ensure_source(
            session,
            country=normalized_country,
            brand=brand,
            raw_pricing_country=str(metadata.get("pricingCountry") or ""),
        )
        spec_highlights = _extract_spec_highlights(record)
        candidate_matches = _build_candidate_matches(
            session,
            country=normalized_country,
            brand=brand,
            model=model,
            trim=trim,
            powertrain="BEV",
        )
        confidence, confidence_rule = _build_confidence_rule(
            record,
            normalized_country,
            selected_market_price,
            spec_highlights,
            has_candidate_matches=bool(candidate_matches),
        )
        source_context = _build_source_context(
            record,
            target_country=normalized_country,
            selected_market_price=selected_market_price,
            spec_highlights=spec_highlights,
        )
        observations.append(
            {
                "source_id": str(source.source_id),
                "country": normalized_country,
                "brand": brand,
                "jato_model": model,
                "jato_trim": trim,
                "jato_powertrain": "BEV",
                "official_model": model,
                "official_trim": trim,
                "official_edition": None,
                "official_powertrain": "BEV",
                "msrp_value": float(selected_market_price["amount"]),
                "currency": str(selected_market_price["currency"]),
                "tax_included": False,
                "price_label": "EVKX reference MSRP",
                "availability_text": str(metadata.get("availabilityFilter") or "current"),
                "observed_at_utc": observed_at,
                "source_url": str(record.get("infoUrl") or ""),
                "source_snapshot_path": str(path),
                "source_payload_hash": _compute_payload_hash(
                    record,
                    selected_market_price=selected_market_price,
                    target_country=normalized_country,
                ),
                "extraction_version": EVKX_EXTRACTOR_VERSION,
                "match_confidence": confidence,
                "match_status": "review_required",
                "match_reason_json": {
                    "profile": EVKX_CONFIDENCE_PROFILE,
                    "source": "EVKX",
                    "vehicleName": display_name,
                    "evId": record.get("evId"),
                    "reviewGate": "review_required_without_cross_source_confirmation",
                    "selectedMarketPrice": selected_market_price,
                    "specHighlights": spec_highlights,
                    "confidenceRule": confidence_rule,
                },
                "source_context_json": source_context,
                "candidate_matches_json": candidate_matches or None,
            }
        )

    unique_brands = sorted(set(scope_brands))
    payload_batch_code = batch_code or (
        f"evkx-{_slugify(normalized_country)}-{observed_at.strftime('%Y%m%d%H%M%S')}"
    )
    return {
        "batch_code": payload_batch_code,
        "trigger_type": "manual_evkx_import",
        "scope_country": normalized_country,
        "scope_brands": unique_brands,
        "failed_count": len(skipped),
        "notes": f"EVKX catalog import from {path}",
        "started_at_utc": observed_at,
        "finished_at_utc": observed_at,
        "observations": observations,
        "skipped": skipped,
        "targetCountry": normalized_country,
    }


def import_evkx_catalog_file(
    session: Session,
    catalog_path: str | Path,
    *,
    target_country: str | None = None,
    batch_code: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    payload = build_evkx_scrape_batch_payload(
        session,
        catalog_path,
        target_country=target_country,
        batch_code=batch_code,
    )
    summary = {
        "targetCountry": payload["targetCountry"],
        "batchCode": payload["batch_code"],
        "scopeBrands": payload["scope_brands"],
        "observationCount": len(payload["observations"]),
        "skippedCount": len(payload["skipped"]),
        "skipped": payload["skipped"][:20],
    }
    if dry_run:
        return {
            **summary,
            "dryRun": True,
            "payloadPreview": {
                "firstObservation": (
                    payload["observations"][0] if payload["observations"] else None
                ),
            },
        }

    ingest_payload = dict(payload)
    ingest_payload.pop("skipped", None)
    ingest_payload.pop("targetCountry", None)
    result = create_scrape_batch_ingest(session, ingest_payload)
    return {
        **summary,
        "dryRun": False,
        "ingestResult": result,
    }
