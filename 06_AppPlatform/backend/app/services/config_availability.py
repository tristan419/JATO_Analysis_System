"""Normalize vehicle configuration cell values into availability states.

The parser extracts raw cell text from the config matrix and classifies
each value into one of six availability states plus an optional normalized
numeric/text value.
"""

from __future__ import annotations

import re
from typing import Literal

Availability = Literal[
    "STANDARD",
    "OPTIONAL",
    "NOT_AVAILABLE",
    "NOT_APPLICABLE",
    "VALUE",
    "UNKNOWN",
]

STANDARD_TOKENS: frozenset[str] = frozenset({"标配", "s", "standard", "标准", "●", "✓", "✔"})
OPTIONAL_TOKENS: frozenset[str] = frozenset({"选装", "o", "optional", "可选", "○", "◯"})
NOT_AVAILABLE_TOKENS: frozenset[str] = frozenset(
    {"-", "无", "不配备", "×", "✘", "✕", "--", "---", "—"}
)
NOT_APPLICABLE_TOKENS: frozenset[str] = frozenset({"n/a", "不适用", "na"})


def _strip_unit_suffix(raw: str) -> tuple[str, str | None]:
    """Detect and strip known unit suffixes like 'km', 'kW', 'mm', etc."""
    match = re.search(
        r"(?<=\d)\s*(L/100km|g/km|kWh|km|kW|mm|kg|L|hp|Nm|s|mph|%)\s*$",
        raw,
        re.IGNORECASE,
    )
    if match:
        unit = match.group(1)
        value_part = raw[: match.start()].strip()
        return value_part, unit
    return raw, None


def classify_availability(raw_value: str | None) -> tuple[Availability, str | None, str | None]:
    """Classify a raw config cell value into availability, normalized value, and unit.

    Returns:
        (availability, normalized_value, unit)
    """
    if raw_value is None:
        return ("UNKNOWN", None, None)

    text = raw_value.strip()
    if not text:
        return ("UNKNOWN", None, None)

    # Equipment markers are not units (in particular S is not seconds).
    marker = re.fullmatch(r"([^（(]+?)\s*([（(].*)?", text, re.DOTALL)
    token = (marker.group(1) if marker else text).strip().casefold()
    note = marker.group(2).strip() if marker and marker.group(2) else ""
    for tokens, availability, label in [
        (STANDARD_TOKENS, "STANDARD", "标配"),
        (OPTIONAL_TOKENS, "OPTIONAL", "选装"),
        (NOT_AVAILABLE_TOKENS, "NOT_AVAILABLE", None),
        (NOT_APPLICABLE_TOKENS, "NOT_APPLICABLE", None),
    ]:
        if token in tokens:
            return (availability, (label or "") + note if note else label, None)

    value_part, unit = _strip_unit_suffix(text)
    return ("VALUE", value_part, unit)
