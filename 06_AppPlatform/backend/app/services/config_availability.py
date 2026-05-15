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

STANDARD_TOKENS: frozenset[str] = frozenset(
    {"标配", "S", "Standard", "standard", "标准", "●", "✓", "✔"}
)
OPTIONAL_TOKENS: frozenset[str] = frozenset(
    {"选装", "O", "Optional", "optional", "可选", "○", "◯"}
)
NOT_AVAILABLE_TOKENS: frozenset[str] = frozenset(
    {"-", "无", "不配备", "×", "✘", "✕", "--", "---", "—"}
)
NOT_APPLICABLE_TOKENS: frozenset[str] = frozenset({"N/A", "不适用", "NA"})


def _strip_unit_suffix(raw: str) -> tuple[str, str | None]:
    """Detect and strip known unit suffixes like 'km', 'kW', 'mm', etc."""
    match = re.search(
        r"\s*(km|kW|kWh|mm|kg|L|hp|Nm|s|mph|g/km|L/100km|%)\s*$",
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

    value_part, unit = _strip_unit_suffix(text)

    if value_part in STANDARD_TOKENS:
        return ("STANDARD", "标配", unit)
    if value_part in OPTIONAL_TOKENS:
        return ("OPTIONAL", "选装", unit)
    if value_part in NOT_AVAILABLE_TOKENS:
        return ("NOT_AVAILABLE", None, unit)
    if value_part in NOT_APPLICABLE_TOKENS:
        return ("NOT_APPLICABLE", None, unit)

    return ("VALUE", value_part, unit)
