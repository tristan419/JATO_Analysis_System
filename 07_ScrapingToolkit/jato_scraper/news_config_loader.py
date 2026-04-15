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


def _build_feed(
    country_code: str,
    country_label: str,
    raw: dict[str, Any],
) -> NewsFeedConfig:
    tags = raw.get("tags") or []
    return NewsFeedConfig(
        source_code=str(raw["source_code"]).strip(),
        country_code=country_code,
        country_label=country_label,
        publisher=str(raw["publisher"]).strip(),
        feed_url=str(raw["feed_url"]).strip(),
        language=str(raw.get("language") or "en").strip() or "en",
        tags=tuple(str(tag).strip() for tag in tags if str(tag).strip()),
    )


def load_news_batch_config(batch_file: str | Path) -> NewsBatchConfig:
    path = Path(batch_file).expanduser().resolve()
    data = _load_yaml_mapping(path)
    countries: list[CountryNewsConfig] = []
    for raw_country in data.get("countries") or []:
        if not isinstance(raw_country, dict):
            continue
        country_code = str(raw_country["country_code"]).strip()
        country_label = str(raw_country["country_label"]).strip()
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
        batch_code=str(data["batch_code"]).strip(),
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
