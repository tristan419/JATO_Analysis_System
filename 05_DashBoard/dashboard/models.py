from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ColumnRegistry:
    country: Optional[str]
    segment: Optional[str]
    powertrain: Optional[str]
    make: Optional[str]
    model: Optional[str]
    version: Optional[str]


@dataclass(frozen=True)
class FilterSelections:
    countries: list[str]
    segments: list[str]
    powertrains: list[str]
    makes: list[str]
    models: list[str]
    versions: list[str]
