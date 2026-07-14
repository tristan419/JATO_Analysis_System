from __future__ import annotations

import re

from sqlalchemy import and_, func


CANONICAL_OFFICIAL_SOURCE_TYPE = "manufacturer_official"
OFFICIAL_SOURCE_TYPE_ALIASES = frozenset(
    {
        CANONICAL_OFFICIAL_SOURCE_TYPE,
        "manufacturer_site",
        "official_website",
        "official_api",
        "official_configurator",
        "official_pdf",
        "official_price_list",
        "official_price_list_pdf",
        "official_web",
    }
)


def source_type_token(value: object | None) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().casefold()).strip(
        "_"
    )


def normalize_msrp_source_type(value: object | None) -> str:
    token = source_type_token(value)
    if token in OFFICIAL_SOURCE_TYPE_ALIASES:
        return CANONICAL_OFFICIAL_SOURCE_TYPE
    return token


def is_official_msrp_source_type(value: object | None) -> bool:
    return normalize_msrp_source_type(value) == CANONICAL_OFFICIAL_SOURCE_TYPE


def is_enabled_official_msrp_source(source: object | None) -> bool:
    return bool(
        source is not None
        and getattr(source, "enabled", False) is True
        and is_official_msrp_source_type(getattr(source, "source_type", None))
    )


def enabled_official_msrp_source_predicate(
    source_type_column,
    enabled_column,
):
    normalized = func.regexp_replace(
        func.lower(func.trim(source_type_column)),
        r"[^a-z0-9]+",
        "_",
        "g",
    )
    return and_(
        enabled_column.is_(True),
        normalized.in_(tuple(sorted(OFFICIAL_SOURCE_TYPE_ALIASES))),
    )
