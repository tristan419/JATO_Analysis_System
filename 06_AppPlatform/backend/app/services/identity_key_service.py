"""Identity key generation and version matching for engineering config."""

from __future__ import annotations


def build_identity_key(
    material_no: str | None = None,
    vehicle_code: str | None = None,
    market: str | None = None,
    model_year: str | None = None,
    trim_name: str | None = None,
) -> str | None:
    parts = [
        (material_no or "").strip(),
        (vehicle_code or "").strip(),
        (market or "").strip(),
        (model_year or "").strip(),
        (trim_name or "").strip(),
    ]
    non_empty = [p for p in parts if p]
    if len(non_empty) < 3:
        return None
    return "|".join(parts)
