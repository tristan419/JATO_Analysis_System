"""Adapters from existing MSRP source YAML files to ScrapeJob contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml

from jato_scraper.core.job import (
    ExtractorKind,
    FetcherKind,
    FreshnessPolicy,
    ScrapeJob,
    canonical_job_id,
)

_EXTRACTOR_KIND_BY_TYPE: dict[str, ExtractorKind] = {
    "http_json": "http_json",
    "scrapling": "scrapling",
    "playwright": "playwright_card_flow",
    "pdf_text": "pdf_text",
}

_FREQUENCY_HOURS: dict[str, int] = {
    "hourly": 1,
    "daily": 24,
    "weekly": 168,
    "manual_only": 168,
    "monthly": 720,
}


def _country_code_from_source_code(source_code: str) -> str:
    normalized = source_code.strip().lower()
    for suffix in (
        "_draft_scrapling",
        "_draft_playwright",
        "_draft_http_json",
        "_draft_pdf_text",
        "_scrapling",
        "_playwright",
        "_http_json",
        "_pdf_text",
    ):
        if normalized.endswith(suffix):
            normalized = normalized.removesuffix(suffix)
            break
    parts = [part for part in normalized.split("_") if part]
    if len(parts) < 2:
        raise ValueError(f"cannot infer country code from source_code={source_code!r}")
    country_code = parts[-1]
    if len(country_code) != 2 or not country_code.isalpha():
        raise ValueError(f"cannot infer country code from source_code={source_code!r}")
    return country_code


def _actual_fetch_url(source: Mapping[str, Any], profile: Mapping[str, Any]) -> str:
    return str(profile.get("url") or source.get("source_url") or "").strip()


def _allow_domains(*urls: str) -> list[str]:
    domains: list[str] = []
    for url in urls:
        parsed = urlparse(url)
        domain = parsed.netloc.strip().lower()
        if domain and domain not in domains:
            domains.append(domain)
    return domains


def _fetcher_for_source(extractor_type: str, profile: Mapping[str, Any]) -> FetcherKind:
    if extractor_type == "http_json":
        return "requests"
    if extractor_type == "pdf_text":
        return "requests"
    if extractor_type == "playwright":
        return "playwright"
    if extractor_type == "scrapling":
        tier = str(profile.get("tier") or "http").strip().lower()
        if tier == "requests":
            return "requests"
        if tier == "dynamic":
            return "playwright"
        return "scrapling"
    raise ValueError(f"unsupported MSRP extractor_type={extractor_type!r}")


def _freshness_for_source(source: Mapping[str, Any]) -> FreshnessPolicy:
    schedule = source.get("schedule") or {}
    if not isinstance(schedule, Mapping):
        return FreshnessPolicy(max_age_hours=168)
    frequency = str(schedule.get("frequency") or "weekly").strip().lower()
    return FreshnessPolicy(max_age_hours=_FREQUENCY_HOURS.get(frequency, 168))


def msrp_source_to_scrape_job(
    source: Mapping[str, Any],
    *,
    source_path: str | Path | None = None,
) -> ScrapeJob:
    """Map one existing MSRP source definition to the unified ScrapeJob schema."""
    profile = source.get("profile") or {}
    if not isinstance(profile, Mapping):
        raise ValueError("MSRP source profile must be a mapping")

    source_code = str(source.get("source_code") or "").strip()
    extractor_type = str(source.get("extractor_type") or "").strip()
    source_url = str(source.get("source_url") or "").strip()
    fetch_url = _actual_fetch_url(source, profile)
    if not source_code:
        raise ValueError("MSRP source is missing source_code")
    if not source_url:
        raise ValueError(f"MSRP source {source_code} is missing source_url")
    if extractor_type not in _EXTRACTOR_KIND_BY_TYPE:
        raise ValueError(f"unsupported MSRP extractor_type={extractor_type!r}")
    if not fetch_url:
        raise ValueError(f"MSRP source {source_code} has no fetch URL")

    country_code = _country_code_from_source_code(source_code)
    metadata: dict[str, Any] = {
        "sourceCode": source_code,
        "country": source.get("country"),
        "countryCode": country_code,
        "brand": source.get("brand"),
        "sourceUrl": source_url,
        "sourceType": source.get("source_type", "manufacturer_official"),
        "priceSemantics": source.get("price_semantics", "base_msrp"),
    }
    if source_path:
        metadata["sourcePath"] = str(source_path)

    return ScrapeJob(
        job_id=canonical_job_id(
            kind="msrp",
            country_code=country_code,
            source_code=source_code,
        ),
        kind="msrp",
        url=fetch_url,
        fetcher=_fetcher_for_source(extractor_type, profile),
        extractor=_EXTRACTOR_KIND_BY_TYPE[extractor_type],
        extractor_config={
            "extractorType": extractor_type,
            "profile": dict(profile),
            "sourceUrl": source_url,
        },
        schema_ref="RawObservation",
        freshness=_freshness_for_source(source),
        priority=80,
        allow_domains=_allow_domains(fetch_url, source_url),
        metadata=metadata,
    )


def load_msrp_scrape_jobs_from_dir(sources_dir: str | Path) -> list[ScrapeJob]:
    """Load all YAML MSRP sources under a directory as ScrapeJob objects."""
    base = Path(sources_dir)
    jobs: list[ScrapeJob] = []
    for path in sorted(base.rglob("*.yaml")):
        relative_parts = path.relative_to(base).parts
        if path.name.startswith("_") or any(part.startswith("_") for part in relative_parts):
            continue
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, Mapping):
            continue
        if not {"source_code", "source_url", "extractor_type", "profile"}.issubset(data):
            continue
        jobs.append(msrp_source_to_scrape_job(data, source_path=path))
    return jobs
