from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import MsrpObservation
from app.infra import msrp_repository, review_repository

MISMATCH_CATEGORY_NAMING = "naming_mismatch"
MISMATCH_CATEGORY_TIMING = "timing_mismatch"
MISMATCH_CATEGORY_MARKET = "market_mismatch"
MISMATCH_CATEGORY_GRANULARITY = "granularity_mismatch"

MISMATCH_CATEGORIES = {
    MISMATCH_CATEGORY_NAMING,
    MISMATCH_CATEGORY_TIMING,
    MISMATCH_CATEGORY_MARKET,
    MISMATCH_CATEGORY_GRANULARITY,
}

RESOLVER_KIND_LINK = "jato_link"
RESOLVER_KIND_OVERRIDE = "match_override"
RESOLVER_KIND_RAW = "observation_payload"

HUMAN_REVIEW_LINK_SOURCE = "review_decision"


def _normalized_text(value: str | None) -> str:
    return (value or "").strip().casefold()


def _token_set(value: str | None) -> set[str]:
    return {
        token
        for token in _normalized_text(value)
        .replace("-", " ")
        .replace("_", " ")
        .split()
        if token
    }


def _safe_decimal_confidence(value: int | None) -> Decimal | None:
    if value is None:
        return None
    bounded = min(max(int(value), 0), 100)
    return Decimal(str(bounded / 100))


def classify_mismatch_category(
    *,
    raw_official_model: str | None,
    raw_official_trim: str | None,
    raw_official_edition: str | None,
    raw_official_powertrain: str | None,
    resolved_official_model: str | None,
    resolved_official_trim: str | None,
    resolved_official_edition: str | None,
    resolved_official_powertrain: str | None,
    resolver_kind: str,
) -> str | None:
    raw_signature = (
        _normalized_text(raw_official_model),
        _normalized_text(raw_official_trim),
        _normalized_text(raw_official_edition),
        _normalized_text(raw_official_powertrain),
    )
    resolved_signature = (
        _normalized_text(resolved_official_model),
        _normalized_text(resolved_official_trim),
        _normalized_text(resolved_official_edition),
        _normalized_text(resolved_official_powertrain),
    )
    if raw_signature == resolved_signature:
        return None
    if resolver_kind == RESOLVER_KIND_OVERRIDE:
        return MISMATCH_CATEGORY_TIMING

    raw_model = raw_signature[0]
    resolved_model = resolved_signature[0]
    raw_powertrain = raw_signature[3]
    resolved_powertrain = resolved_signature[3]
    raw_trim_tokens = _token_set(raw_official_trim)
    resolved_trim_tokens = _token_set(resolved_official_trim)

    if raw_model and resolved_model and raw_model != resolved_model:
        return MISMATCH_CATEGORY_MARKET
    if (
        raw_powertrain != resolved_powertrain
        or bool(raw_official_edition)
        != bool(resolved_official_edition)
    ):
        return MISMATCH_CATEGORY_GRANULARITY
    if (
        raw_trim_tokens
        and resolved_trim_tokens
        and raw_trim_tokens == resolved_trim_tokens
    ):
        return MISMATCH_CATEGORY_NAMING
    if (
        raw_trim_tokens
        and resolved_trim_tokens
        and raw_trim_tokens & resolved_trim_tokens
    ):
        return MISMATCH_CATEGORY_NAMING
    if raw_model == resolved_model:
        return MISMATCH_CATEGORY_NAMING
    return MISMATCH_CATEGORY_MARKET


def resolve_canonical_mapping(
    session: Session,
    *,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
    observed_on: date | None,
    raw_official_model: str | None,
    raw_official_trim: str | None,
    raw_official_edition: str | None,
    raw_official_powertrain: str | None,
) -> dict[str, Any]:
    override = review_repository.find_applicable_override(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        observed_on,
    )
    if override is not None:
        resolved = {
            "resolverKind": RESOLVER_KIND_OVERRIDE,
            "sourceLabel": "override",
            "overrideId": str(override.override_id),
            "linkId": None,
            "confidence": 100,
            "linkSource": HUMAN_REVIEW_LINK_SOURCE,
            "notes": override.override_reason,
            "officialModel": override.official_model,
            "officialTrim": override.official_trim,
            "officialEdition": raw_official_edition,
            "officialPowertrain": raw_official_powertrain,
        }
    else:
        link = msrp_repository.find_active_jato_msrp_link(
            session,
            country,
            brand,
            jato_model,
            jato_trim,
            jato_powertrain,
        )
        if link is not None:
            resolved = {
                "resolverKind": RESOLVER_KIND_LINK,
                "sourceLabel": "link",
                "overrideId": None,
                "linkId": str(link.link_id),
                "confidence": int(link.confidence),
                "linkSource": link.link_source,
                "notes": link.notes,
                "officialModel": link.official_model,
                "officialTrim": link.official_trim,
                "officialEdition": link.official_edition,
                "officialPowertrain": link.official_powertrain,
            }
        else:
            resolved = {
                "resolverKind": RESOLVER_KIND_RAW,
                "sourceLabel": "observation",
                "overrideId": None,
                "linkId": None,
                "confidence": None,
                "linkSource": None,
                "notes": None,
                "officialModel": raw_official_model,
                "officialTrim": raw_official_trim,
                "officialEdition": raw_official_edition,
                "officialPowertrain": raw_official_powertrain,
            }

    mismatch_category = classify_mismatch_category(
        raw_official_model=raw_official_model,
        raw_official_trim=raw_official_trim,
        raw_official_edition=raw_official_edition,
        raw_official_powertrain=raw_official_powertrain,
        resolved_official_model=resolved["officialModel"],
        resolved_official_trim=resolved["officialTrim"],
        resolved_official_edition=resolved["officialEdition"],
        resolved_official_powertrain=resolved["officialPowertrain"],
        resolver_kind=resolved["resolverKind"],
    )
    resolved["mismatchCategory"] = mismatch_category
    return resolved


def apply_canonical_mapping(
    session: Session,
    observation: MsrpObservation,
) -> dict[str, Any]:
    observed_on = (
        observation.observed_at_utc.date()
        if observation.observed_at_utc
        else None
    )
    resolution = resolve_canonical_mapping(
        session,
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain,
        observed_on=observed_on,
        raw_official_model=observation.official_model,
        raw_official_trim=observation.official_trim,
        raw_official_edition=observation.official_edition,
        raw_official_powertrain=observation.official_powertrain,
    )

    previous_status = observation.match_status
    if resolution["resolverKind"] != RESOLVER_KIND_RAW:
        observation.official_model = resolution["officialModel"]
        observation.official_trim = resolution["officialTrim"]
        observation.official_edition = resolution["officialEdition"]
        observation.official_powertrain = resolution["officialPowertrain"]
        mapped_confidence = _safe_decimal_confidence(resolution["confidence"])
        if mapped_confidence is not None:
            current_confidence = observation.match_confidence
            if (
                current_confidence is None
                or mapped_confidence > current_confidence
            ):
                observation.match_confidence = mapped_confidence
        if previous_status == "review_required":
            observation.match_status = (
                "override_applied"
                if resolution["resolverKind"] == RESOLVER_KIND_OVERRIDE
                else "auto_accepted"
            )

    match_reason_json = deepcopy(observation.match_reason_json or {})
    match_reason_json["mappingResolver"] = {
        "resolverKind": resolution["resolverKind"],
        "sourceLabel": resolution["sourceLabel"],
        "linkId": resolution["linkId"],
        "overrideId": resolution["overrideId"],
        "confidence": resolution["confidence"],
        "linkSource": resolution["linkSource"],
        "notes": resolution["notes"],
        "mismatchCategory": resolution["mismatchCategory"],
        "previousStatus": previous_status,
        "resolvedStatus": observation.match_status,
        "resolvedAtUtc": datetime.now(timezone.utc).isoformat(),
        "resolvedOfficialModel": resolution["officialModel"],
        "resolvedOfficialTrim": resolution["officialTrim"],
        "resolvedOfficialEdition": resolution["officialEdition"],
        "resolvedOfficialPowertrain": resolution["officialPowertrain"],
    }
    observation.match_reason_json = match_reason_json
    return resolution


__all__ = [
    "HUMAN_REVIEW_LINK_SOURCE",
    "MISMATCH_CATEGORIES",
    "RESOLVER_KIND_LINK",
    "RESOLVER_KIND_OVERRIDE",
    "RESOLVER_KIND_RAW",
    "apply_canonical_mapping",
    "classify_mismatch_category",
    "resolve_canonical_mapping",
]



