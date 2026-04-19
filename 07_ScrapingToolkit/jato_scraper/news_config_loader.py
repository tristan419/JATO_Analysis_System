"""Load batch news source definitions from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from jato_scraper.news_base import (
    CountryNewsConfig,
    NewsBatchConfig,
    NewsFeedConfig,
)

NEWS_SOURCES_DIR = Path(__file__).resolve().parent.parent / "news_sources"


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


def _build_feed(
    country_code: str,
    country_label: str,
    raw: dict[str, Any],
) -> NewsFeedConfig:
    tags = raw.get("tags") or []
    include_keywords = raw.get("include_keywords") or []
    exclude_keywords = raw.get("exclude_keywords") or []
    return NewsFeedConfig(
        source_code=_require_text_scalar(
            raw["source_code"],
            field_name="source_code",
        ),
        country_code=country_code,
        country_label=country_label,
        publisher=_require_text_scalar(
            raw["publisher"],
            field_name="publisher",
        ),
        feed_url=_require_text_scalar(
            raw["feed_url"],
            field_name="feed_url",
        ),
        language=_require_text_scalar(
            raw.get("language") or "en",
            field_name="language",
        ),
        tags=tuple(str(tag).strip() for tag in tags if str(tag).strip()),
        include_keywords=tuple(
            str(keyword).strip()
            for keyword in include_keywords
            if str(keyword).strip()
        ),
        exclude_keywords=tuple(
            str(keyword).strip()
            for keyword in exclude_keywords
            if str(keyword).strip()
        ),
    )


def load_news_batch_config(batch_file: str | Path) -> NewsBatchConfig:
    path = Path(batch_file).expanduser().resolve()
    data = _load_yaml_mapping(path)
    countries: list[CountryNewsConfig] = []
    for raw_country in data.get("countries") or []:
        if not isinstance(raw_country, dict):
            continue
        country_code = _require_text_scalar(
            raw_country["country_code"],
            field_name="country_code",
        )
        country_label = _require_text_scalar(
            raw_country["country_label"],
            field_name="country_label",
        )
        feeds_raw = raw_country.get("feeds") or []
        feeds = tuple(
            _build_feed(country_code, country_label, raw_feed)
            for raw_feed in feeds_raw
            if isinstance(raw_feed, dict)
        )
        countries.append(
            CountryNewsConfig(
                country_code=country_code,
                country_label=country_label,
                feeds=feeds,
            )
        )
    return NewsBatchConfig(
        batch_code=_require_text_scalar(
            data["batch_code"],
            field_name="batch_code",
        ),
        description=str(data.get("description") or "").strip(),
        countries=tuple(countries),
    )


def load_news_batch_configs(
    sources_dir: str | Path = NEWS_SOURCES_DIR,
) -> list[NewsBatchConfig]:
    base = Path(sources_dir).expanduser().resolve()
    return [
        load_news_batch_config(path)
        for path in sorted(base.glob("*.y*ml"))
    ]
