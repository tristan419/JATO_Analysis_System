"""Unified powertrain normalizer for Order Genius V2.

Used by: material parser, BOM service, FOB service, order sheet service,
and shared with frontend via the powertrain_family API.

Rules:
  - Match longest/most-specific pattern first to avoid substring collisions
    (e.g. PHEV before EV, MHEV before HEV).
  - Normalize to a canonical family name used everywhere:
    backend filtering, frontend filtering, product block colors, analytics.
"""

from __future__ import annotations

import re
from typing import Final

# Order matters: longer / more-specific patterns MUST come first.
_POWERTRAIN_PATTERNS: Final[list[tuple[str, str]]] = [
    # Order matters — exact/specific patterns before short ones
    ("PLUG-IN HYBRID", "PHEV"),
    ("PLUG IN HYBRID", "PHEV"),
    ("PHEV", "PHEV"),
    ("SHS", "PHEV"),
    ("MILD HYBRID", "MHEV"),
    ("MHEV", "MHEV"),
    ("HYBRID ELECTRIC VEHICLE", "HEV"),
    ("HEV", "HEV"),
    ("RANGE EXTENDED", "REEV"),
    ("EREV", "REEV"),
    ("BATTERY ELECTRIC", "BEV"),
    ("BEV", "BEV"),
    ("ELECTRIC", "BEV"),
    ("EV", "BEV"),
    ("FUEL CELL", "FCV"),
    ("FCEV", "FCV"),
    ("LPG", "LPG"),
    ("COMBUSTION", "ICE"),
    ("PETROL", "ICE"),
    ("DIESEL", "ICE"),
    ("GASOLINE", "ICE"),
    ("ICE", "ICE"),
]

# Canonical powertrain → display color (used by frontend PT_COLORS)
POWERTRAIN_COLORS: Final[dict[str, str]] = {
    "BEV": "#16a34a",
    "HEV": "#d97706",
    "PHEV": "#2563eb",
    "MHEV": "#ca8a04",
    "ICE": "#4b5563",
    "LPG": "#6b7280",
    "REEV": "#0d9488",
    "FCV": "#0891b2",
    # Legacy aliases
    "EV": "#16a34a",
    "SHS": "#2563eb",
}


def normalize_powertrain(value: object) -> str:
    """Return canonical powertrain family from a raw value.

    >>> normalize_powertrain("PHEV")
    'PHEV'
    >>> normalize_powertrain("SHS")
    'PHEV'
    >>> normalize_powertrain("PLUG-IN HYBRID")
    'PHEV'
    >>> normalize_powertrain("EV")
    'EV'
    >>> normalize_powertrain("BEV")
    'EV'
    >>> normalize_powertrain("HEV")
    'HEV'
    """
    text = str(value or "").strip().upper()
    if not text or text == "?":
        return "OTHER"
    for pattern, family in _POWERTRAIN_PATTERNS:
        if pattern in text:
            return family
    return text


def normalize_powertrain_market_scan(value: object) -> str:
    """Compatibility wrapper matching the existing _normalize_powertrain.
    Keeps existing behavior for Market Scan (does NOT remap BEV→EV etc.)
    while fixing the HEV/PHEV substring collision.
    """
    text = str(value or "").strip().upper()
    if not text or text == "?":
        return "OTHER"
    if text == "COMBUSTION":
        return "ICE"
    if text == "EREV":
        return "REEV"
    if text == "FCEV":
        return "FCV"
    # For Market Scan, preserve the original label (BEV, PHEV, HEV, etc.)
    # but ensure EV doesn't substring-match inside HEV/PHEV
    for label in ("PHEV", "SHS", "MHEV", "HEV", "BEV", "EV", "ICE"):
        if label in text:
            return label
    return text


def pt_color(family: str) -> str:
    """Return the display color for a powertrain family."""
    return POWERTRAIN_COLORS.get(family.upper(), "#9ca3af")
