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
