"""Base types for country-aware MSRP batch execution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CountryMsrpBatchConfig:
    country_code: str
    country_label: str
    source_refs: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True)
class MsrpBatchConfig:
    batch_code: str
    description: str
    countries: tuple[CountryMsrpBatchConfig, ...]
