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


def _require_text_scalar(value: Any, *, field_name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(
            f"{field_name} must be quoted text in YAML, got boolean {value!r}",
        )
    rendered = str(value).strip()
    if not rendered:
        raise ValueError(f"{field_name} must not be empty")
    return rendered


def _build_source(
    country_code: str,
    country_label: str,
    raw: dict[str, Any],
) -> VocSourceConfig:
    tags = raw.get("tags") or []
    site_type = raw["site_type"] if "site_type" in raw else "forum"
    extractor = raw["extractor"] if "extractor" in raw else "scrapling"
    language = raw["language"] if "language" in raw else "en"
    return VocSourceConfig(
        source_code=_require_text_scalar(
            raw["source_code"],
            field_name="source_code",
        ),
        country_code=country_code,
        country_label=country_label,
        site_name=_require_text_scalar(
            raw["site_name"],
            field_name="site_name",
        ),
        site_url=_require_text_scalar(
            raw["site_url"],
            field_name="site_url",
        ),
        site_type=_require_text_scalar(
            site_type,
            field_name="site_type",
        ),
        extractor=_require_text_scalar(
            extractor,
            field_name="extractor",
        ),
        language=_require_text_scalar(
            language,
            field_name="language",
        ),
        tags=tuple(str(tag).strip() for tag in tags if str(tag).strip()),
        public_access=bool(raw.get("public_access", True)),
        compliance_notes=str(raw.get("compliance_notes") or "").strip(),
        notes=str(raw.get("notes") or "").strip(),
    )


def load_voc_country_config(country_file: str | Path) -> CountryVocConfig:
    path = Path(country_file).expanduser().resolve()
    data = _load_yaml_mapping(path)
    country_code = _require_text_scalar(
        data["country_code"],
        field_name="country_code",
    )
    country_label = _require_text_scalar(
        data["country_label"],
        field_name="country_label",
    )
    languages = tuple(
        _require_text_scalar(item, field_name="languages[]")
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
        country_code = _require_text_scalar(
            raw_country["country_code"],
            field_name="country_code",
        )
        country_label = _require_text_scalar(
            raw_country["country_label"],
            field_name="country_label",
        )
        languages = tuple(
            _require_text_scalar(item, field_name="languages[]")
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
        batch_code=_require_text_scalar(
            data["batch_code"],
            field_name="batch_code",
        ),
        description=str(data.get("description") or "").strip(),
        countries=tuple(countries),
    )


def load_voc_batch_configs(
    sources_dir: str | Path = VOC_SOURCES_DIR,
) -> list[VocBatchConfig]:
    base = Path(sources_dir).expanduser().resolve()
    return [load_voc_batch_config(path) for path in sorted(base.glob("*.y*ml"))]
