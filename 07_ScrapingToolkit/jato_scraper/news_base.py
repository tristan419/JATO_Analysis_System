"""Base types for lightweight country news ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class NewsFeedConfig:
    source_code: str
    country_code: str
    country_label: str
    publisher: str
    feed_url: str
    language: str = "en"
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class CountryNewsConfig:
    country_code: str
    country_label: str
    feeds: tuple[NewsFeedConfig, ...]


@dataclass(frozen=True)
class NewsBatchConfig:
    batch_code: str
    description: str
    countries: tuple[CountryNewsConfig, ...]


@dataclass
class NewsArticle:
    source_code: str
    country_code: str
    country_label: str
    publisher: str
    title: str
    url: str
    summary: str | None = None
    published_at: str | None = None
    tags: tuple[str, ...] = ()
    raw_payload: dict[str, Any] = field(default_factory=dict)
