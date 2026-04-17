"""Base types for country-aware VOC source planning."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VocSourceConfig:
    source_code: str
    country_code: str
    country_label: str
    site_name: str
    site_url: str
    site_type: str = "forum"
    extractor: str = "scrapling"
    language: str = "en"
    tags: tuple[str, ...] = ()
    public_access: bool = True
    compliance_notes: str = ""
    notes: str = ""


@dataclass(frozen=True)
class CountryVocConfig:
    country_code: str
    country_label: str
    languages: tuple[str, ...]
    taxonomy_profile: str
    sources: tuple[VocSourceConfig, ...]


@dataclass(frozen=True)
class VocBatchConfig:
    batch_code: str
    description: str
    countries: tuple[CountryVocConfig, ...]
