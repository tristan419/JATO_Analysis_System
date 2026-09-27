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


def resolve_material_brand(
    brand: object | None,
    model_name: object | None = None,
    bom_template: object | None = None,
) -> str:
    """Return a stored brand, or infer one only from an explicit known identity."""
    stored = normalize_brand(brand)
    if stored:
        return stored
    identity = normalize_brand(" ".join(
        part for part in (clean_text(model_name), clean_text(bom_template)) if part
    ))
    return identity if identity in {"JAECOO", "OMODA"} else ""
