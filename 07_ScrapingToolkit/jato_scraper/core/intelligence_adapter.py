"""Adapters from News/VOC source configs to ScrapeJob contracts."""

from __future__ import annotations

from urllib.parse import urlparse

from jato_scraper.core.job import (
    FreshnessPolicy,
    ScrapeJob,
    canonical_job_id,
)
from jato_scraper.news_base import NewsFeedConfig
from jato_scraper.voc_base import VocSourceConfig


def _allow_domain(url: str) -> list[str]:
    parsed = urlparse(url)
    return [parsed.netloc.strip().lower()] if parsed.netloc else []


def news_feed_to_scrape_job(feed: NewsFeedConfig) -> ScrapeJob:
    """Map one RSS/Atom news feed to the unified ScrapeJob schema."""
    return ScrapeJob(
        job_id=canonical_job_id(
            kind="news",
            country_code=feed.country_code,
            source_code=feed.source_code,
        ),
        kind="news",
        url=feed.feed_url,
        fetcher="requests",
        extractor="rss",
        extractor_config={
            "language": feed.language,
            "includeKeywords": list(feed.include_keywords),
            "excludeKeywords": list(feed.exclude_keywords),
        },
        schema_ref="NewsArticle",
        freshness=FreshnessPolicy(max_age_hours=24),
        priority=60,
        allow_domains=_allow_domain(feed.feed_url),
        metadata={
            "sourceCode": feed.source_code,
            "countryCode": feed.country_code.lower(),
            "country": feed.country_label,
            "publisher": feed.publisher,
            "language": feed.language,
            "tags": list(feed.tags),
        },
    )


def voc_source_to_scrape_job(source: VocSourceConfig) -> ScrapeJob:
    """Map one public VOC source to the unified Scraping Pipeline contract."""
    extractor = source.extractor.strip().lower() or "scrapling"
    fetcher = "playwright" if extractor == "playwright" else "scrapling"
    return ScrapeJob(
        job_id=canonical_job_id(
            kind="voc",
            country_code=source.country_code,
            source_code=source.source_code,
        ),
        kind="voc",
        url=source.site_url,
        fetcher=fetcher,
        extractor="playwright_card_flow" if extractor == "playwright" else "scrapling",
        extractor_config={
            "siteType": source.site_type,
            "language": source.language,
            "publicAccess": source.public_access,
        },
        schema_ref="VocRawDocument",
        freshness=FreshnessPolicy(max_age_hours=168),
        priority=50,
        allow_domains=_allow_domain(source.site_url),
        metadata={
            "sourceCode": source.source_code,
            "countryCode": source.country_code.lower(),
            "country": source.country_label,
            "siteName": source.site_name,
            "siteType": source.site_type,
            "language": source.language,
            "tags": list(source.tags),
            "publicAccess": source.public_access,
            "complianceNotes": source.compliance_notes,
        },
    )
