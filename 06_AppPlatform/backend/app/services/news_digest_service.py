from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from threading import BoundedSemaphore
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.core.config import DATABASE_ENABLED, DATABASE_URL
from app.db.models import CountryNewsArticle
from app.db.models import CountryNewsDigest
from app.db.session import get_session_factory
from app.scraper import enable_external_scraper_package
from app.services import country_profiles


enable_external_scraper_package()

from jato_scraper.news_base import CountryNewsConfig  # noqa: E402
from jato_scraper.news_base import NewsArticle  # noqa: E402
from jato_scraper.news_config_loader import (  # noqa: E402
    load_news_batch_configs,
)
from jato_scraper.news_runner import fetch_feed_articles  # noqa: E402


log = logging.getLogger(__name__)

DEFAULT_NEWS_LIMIT = max(
    1,
    int((os.getenv("APP_COUNTRY_NEWS_LIMIT", "5") or "5").strip()),
)
DEFAULT_TIMEOUT_SECONDS = max(
    5,
    int((os.getenv("APP_COUNTRY_NEWS_TIMEOUT_SECONDS", "10") or "10").strip()),
)
DEFAULT_STALE_AFTER_SECONDS = max(
    3600,
    int(
        (
            os.getenv("APP_COUNTRY_NEWS_STALE_AFTER_SECONDS", "86400")
            or "86400"
        ).strip()
    ),
)
DEFAULT_GEMINI_MODEL = os.getenv(
    "APP_COUNTRY_NEWS_GEMINI_MODEL",
    "gemini-2.5-flash",
).strip()
DEFAULT_GEMINI_TIMEOUT_SECONDS = max(
    5,
    int(
        (
            os.getenv(
                "APP_COUNTRY_NEWS_GEMINI_TIMEOUT_SECONDS",
                "20",
            )
            or "20"
        ).strip()
    ),
)
DEFAULT_GEMINI_MAX_RETRIES = max(
    0,
    int(
        (
            os.getenv(
                "APP_COUNTRY_NEWS_GEMINI_MAX_RETRIES",
                "3",
            )
            or "3"
        ).strip()
    ),
)
DEFAULT_GEMINI_RETRY_BACKOFF_SECONDS = max(
    0.25,
    float(
        (
            os.getenv(
                "APP_COUNTRY_NEWS_GEMINI_RETRY_BACKOFF_SECONDS",
                "1.5",
            )
            or "1.5"
        ).strip()
    ),
)
DEFAULT_GEMINI_MAX_CONCURRENCY = max(
    1,
    int(
        (
            os.getenv(
                "APP_COUNTRY_NEWS_GEMINI_MAX_CONCURRENCY",
                "1",
            )
            or "1"
        ).strip()
    ),
)

_COUNTRY_CONFIGS: dict[str, CountryNewsConfig] | None = None
_COUNTRY_ALIASES: dict[str, str] | None = None
_GEMINI_CONCURRENCY_GUARD = BoundedSemaphore(DEFAULT_GEMINI_MAX_CONCURRENCY)


def _normalize_alias(value: str) -> str:
    return str(value or "").strip().lower()


def _ensure_country_indexes(
) -> tuple[dict[str, CountryNewsConfig], dict[str, str]]:
    global _COUNTRY_ALIASES, _COUNTRY_CONFIGS

    if _COUNTRY_CONFIGS is not None and _COUNTRY_ALIASES is not None:
        return _COUNTRY_CONFIGS, _COUNTRY_ALIASES

    configs: dict[str, CountryNewsConfig] = {}
    aliases: dict[str, str] = {}

    try:
        batches = load_news_batch_configs()
    except Exception as exc:  # noqa: BLE001
        log.warning("Country news config load failed: %s", exc)
        _COUNTRY_CONFIGS = {}
        _COUNTRY_ALIASES = {}
        return _COUNTRY_CONFIGS, _COUNTRY_ALIASES

    for batch in batches:
        for country in batch.countries:
            code = str(country.country_code).strip().upper()
            if not code:
                continue
            configs[code] = country
            for alias in _aliases_for_country(country):
                normalized_alias = _normalize_alias(alias)
                if normalized_alias and normalized_alias not in aliases:
                    aliases[normalized_alias] = code

    _COUNTRY_CONFIGS = configs
    _COUNTRY_ALIASES = aliases
    return configs, aliases


def list_country_news_configs() -> list[CountryNewsConfig]:
    configs, _ = _ensure_country_indexes()
    return [configs[key] for key in sorted(configs)]


def _aliases_for_country(country: CountryNewsConfig) -> set[str]:
    aliases = {
        str(country.country_code).strip(),
        str(country.country_label).strip(),
    }
    label_parts = [
        part.strip()
        for part in str(country.country_label).split("/")
        if part.strip()
    ]
    aliases.update(label_parts)

    english_name = (
        label_parts[0] if label_parts else str(country.country_label)
    )
    profile = country_profiles.get_country_profile(english_name)
    if profile is not None:
        for alias, candidate_profile in (
            country_profiles.COUNTRY_PROFILES.items()
        ):
            if candidate_profile is profile:
                aliases.add(alias)
    return aliases


def resolve_country_news_code(country: str) -> str | None:
    configs, aliases = _ensure_country_indexes()
    normalized_country = str(country or "").strip()
    if not normalized_country:
        return None

    direct = normalized_country.upper()
    if direct in configs:
        return direct
    return aliases.get(_normalize_alias(normalized_country))


def get_country_news_payload(
    country: str,
    *,
    limit: int | None = None,
    allow_live_fetch: bool | None = None,
) -> dict[str, Any]:
    resolved_limit = max(1, int(limit or DEFAULT_NEWS_LIMIT))
    country_code = resolve_country_news_code(country)
    if not country_code:
        return _empty_news_payload(str(country).strip())

    configs, _ = _ensure_country_indexes()
    country_config = configs.get(country_code)
    if country_config is None:
        return _empty_news_payload(
            str(country).strip(),
            country_code=country_code,
        )

    stored_payload = _load_country_news_payload_from_store(
        country_code,
        country_config,
        limit=resolved_limit,
    )
    if stored_payload is not None:
        return stored_payload

    if not _live_fetch_enabled(allow_live_fetch):
        return _empty_news_payload(
            country_config.country_label,
            country_code=country_code,
        )

    articles = _fetch_country_articles(country_config, limit=resolved_limit)
    if not articles:
        return _empty_news_payload(
            country_config.country_label,
            country_code=country_code,
        )

    article_rows = _build_article_rows(
        country_config=country_config,
        articles=articles,
        enrichment=None,
    )
    return _build_public_payload(
        country_code=country_code,
        country_label=country_config.country_label,
        article_rows=article_rows,
        digest_override=None,
        stale=False,
        limit=resolved_limit,
        synced_at=datetime.now(UTC),
        summary_provider="rss-live",
        summary_model=None,
    )


def refresh_country_news(
    country: str,
    *,
    limit: int | None = None,
    persist: bool = True,
    enrich_with_gemini: bool | None = None,
) -> dict[str, Any]:
    resolved_limit = max(1, int(limit or DEFAULT_NEWS_LIMIT))
    country_code = resolve_country_news_code(country)
    if not country_code:
        return _empty_news_payload(str(country).strip())

    configs, _ = _ensure_country_indexes()
    country_config = configs.get(country_code)
    if country_config is None:
        return _empty_news_payload(
            str(country).strip(),
            country_code=country_code,
        )

    if persist and not _database_available():
        raise RuntimeError(
            "Country news sync requires APP_DATABASE_ENABLED=true "
            "and APP_DATABASE_URL",
        )

    articles = _fetch_country_articles(country_config, limit=resolved_limit)
    if not articles:
        stored_payload = _load_country_news_payload_from_store(
            country_code,
            country_config,
            limit=resolved_limit,
        )
        if stored_payload is not None:
            return _mark_payload_stale(stored_payload)
        return _empty_news_payload(
            country_config.country_label,
            country_code=country_code,
        )

    enrichment = None
    if _gemini_enrichment_enabled(enrich_with_gemini):
        enrichment = _generate_gemini_news_digest(country_config, articles)

    article_rows = _build_article_rows(
        country_config=country_config,
        articles=articles,
        enrichment=enrichment,
    )
    synced_at = datetime.now(UTC)
    digest_override = _normalize_digest_override(enrichment)
    payload = _build_public_payload(
        country_code=country_code,
        country_label=country_config.country_label,
        article_rows=article_rows,
        digest_override=digest_override,
        stale=False,
        limit=resolved_limit,
        synced_at=synced_at,
        summary_provider=(
            str(digest_override.get("summaryProvider") or "").strip()
            if digest_override else "rss-fallback"
        ),
        summary_model=(
            str(digest_override.get("summaryModel") or "").strip() or None
            if digest_override else None
        ),
    )

    if persist:
        with _session_scope() as session:
            _upsert_country_news_payload(
                session,
                payload=payload,
                article_rows=article_rows,
                synced_at=synced_at,
            )
            session.commit()
    return payload


def get_country_news_ops_status(country: str) -> dict[str, Any]:
    normalized_country = str(country or "").strip()
    country_code = resolve_country_news_code(normalized_country)
    configs, _ = _ensure_country_indexes()
    country_config = configs.get(country_code or "") if country_code else None
    country_label = (
        country_config.country_label
        if country_config is not None
        else normalized_country
    )

    stored_payload = None
    if country_code and country_config is not None:
        stored_payload = _load_country_news_payload_from_store(
            country_code,
            country_config,
            limit=DEFAULT_NEWS_LIMIT,
        )

    news_digest = (
        stored_payload.get("newsDigest")
        if isinstance(stored_payload, dict)
        else None
    )
    market_events = (
        stored_payload.get("marketEvents")
        if isinstance(stored_payload, dict)
        else []
    )
    article_count = 0
    if isinstance(news_digest, dict):
        article_count = int(news_digest.get("articleCount") or 0)
    elif isinstance(market_events, list):
        article_count = len(market_events)

    return {
        "country": normalized_country,
        "countryCode": country_code,
        "countryLabel": country_label,
        "configured": country_config is not None,
        "feedCount": (
            len(country_config.feeds)
            if country_config is not None
            else 0
        ),
        "databaseEnabled": _database_available(),
        "hasSnapshot": bool(news_digest or market_events),
        "articleCount": article_count,
        "syncTimestamp": (
            str(news_digest.get("syncTimestamp") or "").strip()
            if isinstance(news_digest, dict)
            else None
        ) or None,
        "updatedAt": (
            str(news_digest.get("updatedAt") or "").strip()
            if isinstance(news_digest, dict)
            else None
        ) or None,
        "summaryProvider": (
            str(news_digest.get("summaryProvider") or "").strip()
            if isinstance(news_digest, dict)
            else None
        ) or None,
        "summaryModel": (
            str(news_digest.get("summaryModel") or "").strip()
            if isinstance(news_digest, dict)
            else None
        ) or None,
        "stale": (
            bool(news_digest.get("stale"))
            if isinstance(news_digest, dict)
            else None
        ),
        "liveFetchDefaultEnabled": _live_fetch_enabled(None),
        "onlineRefreshSupported": country_config is not None,
        "geminiConfigured": bool(_gemini_api_key()),
        "geminiModel": DEFAULT_GEMINI_MODEL if _gemini_api_key() else None,
    }


def _database_available() -> bool:
    return bool(DATABASE_ENABLED and DATABASE_URL)


@contextmanager
def _session_scope() -> Iterator[Session]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def _load_country_news_payload_from_store(
    country_code: str,
    country_config: CountryNewsConfig,
    *,
    limit: int,
) -> dict[str, Any] | None:
    if not _database_available():
        return None

    with _session_scope() as session:
        digest = session.execute(
            select(CountryNewsDigest).where(
                CountryNewsDigest.country_code == country_code,
            )
        ).scalar_one_or_none()
        articles = session.execute(
            select(CountryNewsArticle)
            .where(CountryNewsArticle.country_code == country_code)
            .order_by(
                CountryNewsArticle.published_at_utc.desc().nullslast(),
                CountryNewsArticle.synced_at_utc.desc(),
                CountryNewsArticle.updated_at_utc.desc(),
            )
            .limit(max(limit, DEFAULT_NEWS_LIMIT))
        ).scalars().all()

    if digest is None and not articles:
        return None

    synced_at = None
    if digest is not None:
        synced_at = digest.synced_at_utc
    elif articles:
        synced_at = articles[0].synced_at_utc

    stale = _is_stale(synced_at)
    article_rows = [_article_row_from_model(article) for article in articles]
    digest_override = _digest_override_from_model(digest) if digest else None
    summary_provider = (
        str(digest.summary_provider or "").strip()
        if digest is not None else "rss-stored"
    )
    summary_model = (
        str(digest.summary_model or "").strip() or None
        if digest is not None else None
    )
    return _build_public_payload(
        country_code=country_code,
        country_label=(
            digest.country_label
            if digest is not None
            else country_config.country_label
        ),
        article_rows=article_rows,
        digest_override=digest_override,
        stale=stale,
        limit=limit,
        synced_at=synced_at,
        summary_provider=summary_provider,
        summary_model=summary_model,
    )


def _upsert_country_news_payload(
    session: Session,
    *,
    payload: dict[str, Any],
    article_rows: list[dict[str, Any]],
    synced_at: datetime,
) -> None:
    news_digest = payload.get("newsDigest") or {}
    digest_insert = insert(CountryNewsDigest).values(
        country_news_digest_id=_new_uuid_string(),
        country_code=payload.get("countryCode"),
        country_label=payload.get("countryLabel"),
        article_count=int(
            news_digest.get("articleCount") or len(article_rows)
        ),
        published_at_utc=_parse_datetime(news_digest.get("updatedAt")),
        synced_at_utc=synced_at,
        headline=news_digest.get("headline"),
        summary=news_digest.get("summary"),
        highlights_json=_json_safe(news_digest.get("highlights") or []),
        summary_provider=news_digest.get("summaryProvider"),
        summary_model=news_digest.get("summaryModel"),
    )
    session.execute(
        digest_insert.on_conflict_do_update(
            index_elements=["country_code"],
            set_={
                "country_label": digest_insert.excluded.country_label,
                "article_count": digest_insert.excluded.article_count,
                "published_at_utc": digest_insert.excluded.published_at_utc,
                "synced_at_utc": digest_insert.excluded.synced_at_utc,
                "headline": digest_insert.excluded.headline,
                "summary": digest_insert.excluded.summary,
                "highlights_json": digest_insert.excluded.highlights_json,
                "summary_provider": digest_insert.excluded.summary_provider,
                "summary_model": digest_insert.excluded.summary_model,
                "updated_at_utc": synced_at,
            },
        )
    )

    for article in article_rows:
        article_insert = insert(CountryNewsArticle).values(
            country_news_article_id=_new_uuid_string(),
            country_code=article.get("countryCode"),
            country_label=article.get("countryLabel"),
            source_code=article.get("sourceCode"),
            publisher=article.get("publisher"),
            title=article.get("title"),
            source_url=article.get("url"),
            raw_summary=article.get("rawSummary"),
            summary=article.get("summary"),
            published_at_utc=article.get("publishedAtUtc"),
            tags_json=_json_safe(article.get("tags") or []),
            raw_payload_json=_json_safe(article.get("rawPayload") or {}),
            intelligence_provider=article.get("summaryProvider"),
            intelligence_model=article.get("summaryModel"),
            synced_at_utc=synced_at,
        )
        session.execute(
            article_insert.on_conflict_do_update(
                index_elements=["country_code", "source_url"],
                set_={
                    "country_label": article_insert.excluded.country_label,
                    "source_code": article_insert.excluded.source_code,
                    "publisher": article_insert.excluded.publisher,
                    "title": article_insert.excluded.title,
                    "raw_summary": article_insert.excluded.raw_summary,
                    "summary": article_insert.excluded.summary,
                    "published_at_utc": (
                        article_insert.excluded.published_at_utc
                    ),
                    "tags_json": article_insert.excluded.tags_json,
                    "raw_payload_json": (
                        article_insert.excluded.raw_payload_json
                    ),
                    "intelligence_provider": (
                        article_insert.excluded.intelligence_provider
                    ),
                    "intelligence_model": (
                        article_insert.excluded.intelligence_model
                    ),
                    "synced_at_utc": article_insert.excluded.synced_at_utc,
                    "updated_at_utc": synced_at,
                },
            )
        )


def _build_article_rows(
    *,
    country_config: CountryNewsConfig,
    articles: list[NewsArticle],
    enrichment: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    event_overrides: dict[str, dict[str, Any]] = {}
    if isinstance(enrichment, dict):
        for item in enrichment.get("events") or []:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if url:
                event_overrides[url] = item

    provider = (
        str(enrichment.get("provider") or "").strip()
        if isinstance(enrichment, dict) else ""
    ) or "rss-fallback"
    model = (
        str(enrichment.get("model") or "").strip() or None
        if isinstance(enrichment, dict) else None
    )

    rows: list[dict[str, Any]] = []
    for article in articles:
        override = event_overrides.get(str(article.url or "").strip(), {})
        published_at_utc = _parse_datetime(article.published_at)
        published_at = _format_public_datetime(
            article.published_at,
            published_at_utc,
        )
        raw_summary = str(article.summary or "").strip() or None
        summary = str(override.get("summary") or "").strip() or raw_summary
        tags = _normalize_tag_list(override.get("tags"))
        if not tags:
            tags = _normalize_tag_list(article.tags)
        rows.append(
            {
                "sourceCode": article.source_code,
                "countryCode": article.country_code,
                "countryLabel": article.country_label,
                "publisher": article.publisher,
                "title": article.title,
                "summary": summary,
                "rawSummary": raw_summary,
                "url": article.url,
                "publishedAt": published_at,
                "publishedAtUtc": published_at_utc,
                "tags": tags,
                "rawPayload": _json_safe(article.raw_payload),
                "summaryProvider": provider,
                "summaryModel": model,
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            row.get("publishedAtUtc") or datetime.min.replace(tzinfo=UTC),
            str(row.get("title") or "").lower(),
        ),
        reverse=True,
    )


def _build_public_payload(
    *,
    country_code: str,
    country_label: str,
    article_rows: list[dict[str, Any]],
    digest_override: dict[str, Any] | None,
    stale: bool,
    limit: int,
    synced_at: datetime | None,
    summary_provider: str,
    summary_model: str | None,
) -> dict[str, Any]:
    public_events = [
        _public_market_event(row)
        for row in article_rows[:limit]
    ]
    news_digest = _build_news_digest(
        country_code=country_code,
        country_label=country_label,
        market_events=public_events,
        stale=stale,
        digest_override=digest_override,
        synced_at=synced_at,
        summary_provider=summary_provider,
        summary_model=summary_model,
    )
    return {
        "countryCode": country_code,
        "countryLabel": country_label,
        "marketEvents": public_events,
        "newsDigest": news_digest,
    }


def _public_market_event(article_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceCode": article_row.get("sourceCode"),
        "countryCode": article_row.get("countryCode"),
        "countryLabel": article_row.get("countryLabel"),
        "publisher": article_row.get("publisher"),
        "title": article_row.get("title"),
        "summary": article_row.get("summary"),
        "url": article_row.get("url"),
        "publishedAt": article_row.get("publishedAt"),
        "tags": list(article_row.get("tags") or []),
    }


def _build_news_digest(
    *,
    country_code: str,
    country_label: str,
    market_events: list[dict[str, Any]],
    stale: bool,
    digest_override: dict[str, Any] | None,
    synced_at: datetime | None,
    summary_provider: str,
    summary_model: str | None,
) -> dict[str, Any] | None:
    if not market_events and not digest_override:
        return None

    fallback_highlights: list[str] = []
    for event in market_events[:3]:
        highlight = _format_event_highlight(event)
        if highlight:
            fallback_highlights.append(highlight)
    updated_at = next(
        (
            str(event.get("publishedAt") or "").strip()
            for event in market_events
            if str(event.get("publishedAt") or "").strip()
        ),
        None,
    ) or _datetime_to_iso(synced_at)

    override = digest_override or {}
    headline = str(override.get("headline") or "").strip()
    summary = str(override.get("summary") or "").strip()
    highlights = _normalize_highlights(override.get("highlights"))
    provider = str(override.get("summaryProvider") or summary_provider).strip()
    model = (
        str(override.get("summaryModel") or summary_model or "").strip()
        or None
    )

    if not highlights:
        highlights = fallback_highlights
    if not headline:
        headline = (
            highlights[0]
            if highlights else str(market_events[0].get("title") or "").strip()
        )
    if not summary:
        summary = "；".join(highlights)

    return {
        "countryCode": country_code,
        "countryLabel": country_label,
        "articleCount": len(market_events),
        "updatedAt": updated_at,
        "headline": headline,
        "summary": summary,
        "highlights": highlights,
        "stale": stale,
        "summaryProvider": provider or None,
        "summaryModel": model,
        "syncTimestamp": _datetime_to_iso(synced_at),
    }


def _normalize_digest_override(
    enrichment: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(enrichment, dict):
        return None
    headline = str(enrichment.get("headline") or "").strip()
    summary = str(enrichment.get("summary") or "").strip()
    highlights = _normalize_highlights(enrichment.get("highlights"))
    provider = str(enrichment.get("provider") or "").strip()
    model = str(enrichment.get("model") or "").strip() or None
    if not any((headline, summary, highlights)):
        return None
    return {
        "headline": headline,
        "summary": summary,
        "highlights": highlights,
        "summaryProvider": provider or None,
        "summaryModel": model,
    }


def _digest_override_from_model(
    digest: CountryNewsDigest,
) -> dict[str, Any] | None:
    highlights = _normalize_highlights(digest.highlights_json)
    headline = str(digest.headline or "").strip()
    summary = str(digest.summary or "").strip()
    if not any((headline, summary, highlights)):
        return None
    return {
        "headline": headline,
        "summary": summary,
        "highlights": highlights,
        "summaryProvider": str(digest.summary_provider or "").strip() or None,
        "summaryModel": str(digest.summary_model or "").strip() or None,
    }


def _article_row_from_model(article: CountryNewsArticle) -> dict[str, Any]:
    return {
        "sourceCode": article.source_code,
        "countryCode": article.country_code,
        "countryLabel": article.country_label,
        "publisher": article.publisher,
        "title": article.title,
        "summary": article.summary,
        "rawSummary": article.raw_summary,
        "url": article.source_url,
        "publishedAt": _datetime_to_iso(article.published_at_utc),
        "publishedAtUtc": article.published_at_utc,
        "tags": _normalize_tag_list(article.tags_json),
        "rawPayload": article.raw_payload_json or {},
        "summaryProvider": article.intelligence_provider,
        "summaryModel": article.intelligence_model,
    }


def _mark_payload_stale(payload: dict[str, Any]) -> dict[str, Any]:
    cloned_payload = dict(payload)
    news_digest = payload.get("newsDigest")
    if isinstance(news_digest, dict):
        cloned_digest = dict(news_digest)
        cloned_digest["stale"] = True
        cloned_payload["newsDigest"] = cloned_digest
    return cloned_payload


def _empty_news_payload(
    country_label: str,
    *,
    country_code: str | None = None,
) -> dict[str, Any]:
    return {
        "countryCode": country_code,
        "countryLabel": country_label,
        "marketEvents": [],
        "newsDigest": None,
    }


def _live_fetch_enabled(allow_live_fetch: bool | None) -> bool:
    if allow_live_fetch is not None:
        return allow_live_fetch

    raw = os.getenv("APP_COUNTRY_NEWS_LIVE_FETCH", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _gemini_enrichment_enabled(explicit: bool | None) -> bool:
    if explicit is not None:
        return explicit and bool(_gemini_api_key())

    raw = os.getenv("APP_COUNTRY_NEWS_GEMINI_ENABLED", "").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    if raw in {"1", "true", "yes", "on"}:
        return bool(_gemini_api_key())
    return bool(_gemini_api_key())


def _gemini_api_key() -> str:
    return (
        os.getenv("GEMINI_API_KEY", "").strip()
        or os.getenv("GOOGLE_API_KEY", "").strip()
    )


def _fetch_country_articles(
    country_config: CountryNewsConfig,
    *,
    limit: int,
) -> list[NewsArticle]:
    collected: list[NewsArticle] = []
    seen_urls: set[str] = set()

    for feed in country_config.feeds:
        try:
            articles = fetch_feed_articles(
                feed,
                limit_per_feed=limit,
                timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Country news feed failed for %s/%s: %s",
                country_config.country_code,
                feed.source_code,
                exc,
            )
            continue

        for article in articles:
            if article.url in seen_urls:
                continue
            seen_urls.add(article.url)
            collected.append(article)

    return sorted(
        collected,
        key=lambda article: (
            _parse_datetime(article.published_at)
            or datetime.min.replace(tzinfo=UTC),
            str(article.title or "").lower(),
        ),
        reverse=True,
    )[: max(limit, DEFAULT_NEWS_LIMIT)]


def _generate_gemini_news_digest(
    country_config: CountryNewsConfig,
    articles: list[NewsArticle],
) -> dict[str, Any] | None:
    api_key = _gemini_api_key()
    if not api_key or not articles:
        return None

    url = (
        "https://generativelanguage.googleapis.com/v1beta/"
        f"models/{DEFAULT_GEMINI_MODEL}:generateContent?key={api_key}"
    )
    request_body = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": _build_gemini_prompt(country_config, articles),
                    }
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }
    request = Request(
        url,
        data=json.dumps(request_body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    payload: dict[str, Any] | None = None
    with _GEMINI_CONCURRENCY_GUARD:
        for attempt in range(DEFAULT_GEMINI_MAX_RETRIES + 1):
            try:
                with urlopen(
                    request,
                    timeout=DEFAULT_GEMINI_TIMEOUT_SECONDS,
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                if attempt >= DEFAULT_GEMINI_MAX_RETRIES or not _is_retryable_gemini_error(
                    exc
                ):
                    log.warning(
                        "Gemini news digest failed for %s: %s",
                        country_config.country_code,
                        exc,
                    )
                    return None

                backoff_seconds = _gemini_retry_delay_seconds(exc, attempt)
                log.info(
                    "Gemini news digest retry %s/%s for %s after %.2fs: %s",
                    attempt + 1,
                    DEFAULT_GEMINI_MAX_RETRIES,
                    country_config.country_code,
                    backoff_seconds,
                    exc,
                )
                time.sleep(backoff_seconds)

    if payload is None:
        return None

    text = _extract_gemini_response_text(payload)
    if not text:
        return None
    try:
        parsed = _parse_json_object(text)
    except ValueError as exc:
        log.warning(
            "Gemini news digest JSON parse failed for %s: %s",
            country_config.country_code,
            exc,
        )
        return None

    return {
        "provider": "gemini",
        "model": DEFAULT_GEMINI_MODEL,
        "headline": str(parsed.get("headline") or "").strip(),
        "summary": str(parsed.get("summary") or "").strip(),
        "highlights": _normalize_highlights(parsed.get("highlights")),
        "events": _normalize_event_overrides(parsed.get("events")),
    }


def _is_retryable_gemini_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code == 429 or 500 <= exc.code < 600
    if isinstance(exc, (URLError, TimeoutError)):
        return True
    return False


def _gemini_retry_delay_seconds(exc: Exception, attempt: int) -> float:
    if isinstance(exc, HTTPError):
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        parsed_retry_after = _parse_retry_after_seconds(retry_after)
        if parsed_retry_after is not None:
            return max(
                parsed_retry_after,
                DEFAULT_GEMINI_RETRY_BACKOFF_SECONDS,
            )
    return DEFAULT_GEMINI_RETRY_BACKOFF_SECONDS * (2**attempt)


def _parse_retry_after_seconds(value: str | None) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _build_gemini_prompt(
    country_config: CountryNewsConfig,
    articles: list[NewsArticle],
) -> str:
    article_payload = []
    for article in articles[:DEFAULT_NEWS_LIMIT]:
        article_payload.append(
            {
                "url": article.url,
                "publisher": article.publisher,
                "title": article.title,
                "summary": article.summary,
                "publishedAt": article.published_at,
                "tags": list(article.tags),
            }
        )

    return (
        "You are an automotive market intelligence analyst.\n"
        "Given the country news articles below, return JSON only.\n"
        "Write the digest in Simplified Chinese.\n"
        "Schema:\n"
        "{\n"
        '  "headline": string,\n'
        '  "summary": string,\n'
        '  "highlights": [string, string, string],\n'
        '  "events": [\n'
        "    {\n"
        '      "url": string,\n'
        '      "summary": string,\n'
        '      "tags": [string]\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Rules:\n"
        "- Focus on pricing, policy, incentives, tariffs, competition, "
        "electrification, production, launches, fleet.\n"
        "- Keep each event summary under 40 Chinese characters when "
        "possible.\n"
        "- highlights max 3 items.\n"
        "- tags should be short lowercase English labels.\n"
        "- Never invent URLs.\n"
        f"Country: {country_config.country_label} "
        f"({country_config.country_code})\n"
        f"Articles: {json.dumps(article_payload, ensure_ascii=False)}"
    )


def _extract_gemini_response_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return ""
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        text_parts: list[str] = []
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(text.strip())
        if text_parts:
            return "\n".join(text_parts)
    return ""


def _normalize_event_overrides(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        normalized.append(
            {
                "url": url,
                "summary": str(item.get("summary") or "").strip(),
                "tags": _normalize_tag_list(item.get("tags")),
            }
        )
    return normalized


def _normalize_highlights(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        str(item).strip()
        for item in value
        if str(item).strip()
    ][:3]


def _normalize_tag_list(value: Any) -> list[str]:
    if isinstance(value, tuple):
        value = list(value)
    if not isinstance(value, list):
        return []
    tags: list[str] = []
    seen: set[str] = set()
    for item in value:
        tag = str(item or "").strip().lower()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tags


def _format_event_highlight(event: dict[str, Any]) -> str:
    title = str(event.get("title") or "").strip()
    if not title:
        return ""
    publisher = str(event.get("publisher") or "").strip()
    published_at = str(event.get("publishedAt") or "").strip()
    date_prefix = (
        published_at[:10]
        if len(published_at) >= 10
        else published_at
    )
    prefix = " ".join(
        part for part in (date_prefix, publisher) if part
    ).strip()
    return f"{prefix}: {title}" if prefix else title


def _is_stale(synced_at: datetime | None) -> bool:
    if synced_at is None:
        return False
    normalized = synced_at
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=UTC)
    return (datetime.now(UTC) - normalized) > timedelta(
        seconds=DEFAULT_STALE_AFTER_SECONDS,
    )


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_public_datetime(
    raw_value: Any,
    parsed_value: datetime | None,
) -> str | None:
    if parsed_value is not None:
        return parsed_value.isoformat()
    text = str(raw_value or "").strip()
    return text or None


def _datetime_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=UTC)
    return normalized.isoformat()


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:]
        stripped = stripped.strip()
    parsed = json.loads(stripped)
    if not isinstance(parsed, dict):
        raise ValueError("expected JSON object")
    return parsed


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return _datetime_to_iso(value)
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
        }
    return str(value)


def _new_uuid_string() -> Any:
    from uuid import uuid4

    return uuid4()
