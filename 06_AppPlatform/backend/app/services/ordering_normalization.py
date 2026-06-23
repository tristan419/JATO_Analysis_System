"""Shared normalization helpers for Ordering / BOM entities."""

from __future__ import annotations

import re


def clean_text(value: object | None) -> str:
    return str(value or "").strip()


def normalize_brand_text(value: object | None) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.sub("JEACOO", "JAECOO", text, flags=re.IGNORECASE)
    text = re.sub("JECOO", "JAECOO", text, flags=re.IGNORECASE)
    return text


def normalize_brand(value: object | None) -> str:
    text = normalize_brand_text(value)
    if not text:
        return ""
    collapsed = re.sub(r"[^A-Z0-9]", "", text.upper())
    if "JAECOO" in collapsed:
        return "JAECOO"
    if "OMODA" in collapsed:
        return "OMODA"
    return text.upper()


_COLOUR_TIER_RANK = {
    "single": 0,
    "dual": 1,
    "special": 2,
}


def normalize_colour_tier(value: object | None) -> str:
    text = clean_text(value).lower().replace("_", "-")
    if text in {"dual", "two-tone", "dual-tone", "dual tone", "bi-color", "bi-colour"}:
        return "dual"
    if text in {"special", "matte", "black edition", "pearl", "metallic"}:
        return "special"
    return "single"


def infer_colour_tier(
    colour_name: object | None,
    colour_type: object | None = None,
    edition_tag: object | None = None,
) -> str:
    """Infer the minimum paint tier from colour metadata."""
    name = clean_text(colour_name).lower()
    ctype = clean_text(colour_type).lower().replace("_", "-")
    edition = clean_text(edition_tag).lower()
    if (
        edition
        or "black edition" in name
        or "matte" in name
        or ctype in {"special", "matte", "pearl", "metallic"}
    ):
        return "special"
    if (
        ctype in {"dual", "two-tone", "dual-tone", "dual tone", "bi-color", "bi-colour"}
        or re.search(r"[/&／]", name)
        or "双色" in name
        or "dual" in name
        or "two tone" in name
        or "two-tone" in name
        or "black roof" in name
        or re.search(r"bi.?colou?r", name)
    ):
        return "dual"
    return "single"


def merge_colour_tiers(*tiers: object | None) -> str:
    best = "single"
    for tier in tiers:
        normalized = normalize_colour_tier(tier)
        if _COLOUR_TIER_RANK[normalized] > _COLOUR_TIER_RANK[best]:
            best = normalized
    return best
