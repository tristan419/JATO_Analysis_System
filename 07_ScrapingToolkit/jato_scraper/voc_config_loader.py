"""Load country-aware VOC source definitions from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from jato_scraper.voc_base import CountryVocConfig, VocBatchConfig, VocSourceConfig

VOC_SOURCES_DIR = Path(__file__).resolve().parent.parent / "voc_sources"


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not parse into a YAML mapping")
    return data


def _build_source(
    country_code: str,
    country_label: str,
    raw: dict[str, Any],
) -> VocSourceConfig:
    tags = raw.get("tags") or []
    return VocSourceConfig(
        source_code=str(raw["source_code"]).strip(),
        country_code=country_code,
        country_label=country_label,
        site_name=str(raw["site_name"]).strip(),
        site_url=str(raw["site_url"]).strip(),
        site_type=str(raw.get("site_type") or "forum").strip() or "forum",
        extractor=str(raw.get("extractor") or "scrapling").strip() or "scrapling",
        language=str(raw.get("language") or "en").strip() or "en",
        tags=tuple(str(tag).strip() for tag in tags if str(tag).strip()),
        public_access=bool(raw.get("public_access", True)),
        compliance_notes=str(raw.get("compliance_notes") or "").strip(),
        notes=str(raw.get("notes") or "").strip(),
    )


def load_voc_country_config(country_file: str | Path) -> CountryVocConfig:
    path = Path(country_file).expanduser().resolve()
    data = _load_yaml_mapping(path)
    country_code = str(data["country_code"]).strip()
    country_label = str(data["country_label"]).strip()
    languages = tuple(
        str(item).strip()
        for item in (data.get("languages") or [])
        if str(item).strip()
    )
    sources = tuple(
        _build_source(country_code, country_label, raw_source)
        for raw_source in (data.get("sources") or [])
        if isinstance(raw_source, dict)
    )
    return CountryVocConfig(
        country_code=country_code,
        country_label=country_label,
        languages=languages,
        taxonomy_profile=str(data.get("taxonomy_profile") or "").strip(),
        sources=sources,
    )


def load_voc_batch_config(batch_file: str | Path) -> VocBatchConfig:
    path = Path(batch_file).expanduser().resolve()
    data = _load_yaml_mapping(path)
    countries: list[CountryVocConfig] = []
    for raw_country in data.get("countries") or []:
        if not isinstance(raw_country, dict):
            continue
        country_file = raw_country.get("country_file")
        if country_file:
            country_path = (path.parent / str(country_file)).resolve()
            countries.append(load_voc_country_config(country_path))
            continue
        country_code = str(raw_country["country_code"]).strip()
        country_label = str(raw_country["country_label"]).strip()
        languages = tuple(
            str(item).strip()
            for item in (raw_country.get("languages") or [])
            if str(item).strip()
        )
        sources = tuple(
            _build_source(country_code, country_label, raw_source)
            for raw_source in (raw_country.get("sources") or [])
            if isinstance(raw_source, dict)
        )
        countries.append(
            CountryVocConfig(
                country_code=country_code,
                country_label=country_label,
                languages=languages,
                taxonomy_profile=str(raw_country.get("taxonomy_profile") or "").strip(),
                sources=sources,
            )
        )
    return VocBatchConfig(
        batch_code=str(data["batch_code"]).strip(),
        description=str(data.get("description") or "").strip(),
        countries=tuple(countries),
    )


def load_voc_batch_configs(
    sources_dir: str | Path = VOC_SOURCES_DIR,
) -> list[VocBatchConfig]:
    base = Path(sources_dir).expanduser().resolve()
    return [load_voc_batch_config(path) for path in sorted(base.glob("*.y*ml"))]
