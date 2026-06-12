"""Adapters for policy, incentive, and spec source configs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Mapping

import yaml

from jato_scraper.core.job import (
    ExtractorKind,
    FetcherKind,
    FreshnessPolicy,
    ScrapeJob,
    canonical_job_id,
)


DomainJobKind = Literal["policy", "incentive", "spec"]

_SCHEMA_REF_BY_KIND: dict[DomainJobKind, str] = {
    "policy": "PolicySnippet",
    "incentive": "IncentiveProgram",
    "spec": "SpecFeatureObservation",
}
_DEFAULT_FETCHER_BY_KIND: dict[DomainJobKind, FetcherKind] = {
    "policy": "requests",
    "incentive": "requests",
    "spec": "scrapling",
}
_DEFAULT_EXTRACTOR_BY_KIND: dict[DomainJobKind, ExtractorKind] = {
    "policy": "llm_extract",
    "incentive": "llm_extract",
    "spec": "css_rules",
}
_DEFAULT_FRESHNESS_HOURS_BY_KIND: dict[DomainJobKind, int] = {
    "policy": 720,
    "incentive": 168,
    "spec": 168,
}
_DEFAULT_PRIORITY_BY_KIND: dict[DomainJobKind, int] = {
    "policy": 55,
    "incentive": 65,
    "spec": 70,
}


def _require_text(value: Any, *, field_name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be quoted text, got boolean {value!r}")
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError(f"{field_name} must not be empty")
    return rendered


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_items = [value]
    else:
        raw_items = list(value)
    return [
        str(item).strip()
        for item in raw_items
        if str(item).strip()
    ]


def _optional_int(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    return int(value)


def domain_source_to_scrape_job(
    source: Mapping[str, Any],
    *,
    kind: DomainJobKind,
    country_code: str,
    country_label: str,
    batch_code: str | None = None,
    source_path: str | Path | None = None,
) -> ScrapeJob:
    """Map one policy/incentive/spec source to the unified ScrapeJob schema."""
    source_code = _require_text(source.get("source_code"), field_name="source_code")
    url = _require_text(source.get("url"), field_name="url")
    fetcher = str(source.get("fetcher") or _DEFAULT_FETCHER_BY_KIND[kind]).strip()
    extractor = str(
        source.get("extractor") or _DEFAULT_EXTRACTOR_BY_KIND[kind]
    ).strip()
    freshness_hours = _optional_int(
        source.get("freshness_hours"),
        default=_DEFAULT_FRESHNESS_HOURS_BY_KIND[kind],
    )
    priority = _optional_int(
        source.get("priority"),
        default=_DEFAULT_PRIORITY_BY_KIND[kind],
    )
    extractor_config = dict(source.get("extractor_config") or {})
    metadata: dict[str, Any] = {
        "sourceCode": source_code,
        "countryCode": country_code.strip().lower(),
        "country": country_label,
        "sourceName": source.get("source_name") or source.get("title"),
        "sourceKind": source.get("source_kind"),
        "topics": _string_list(source.get("topics")),
        "tags": _string_list(source.get("tags")),
    }
    if batch_code:
        metadata["batchCode"] = batch_code
    for key in (
        "brand",
        "model",
        "official_model",
        "program_name",
        "citation_tier",
        "applies_to",
    ):
        if source.get(key) is not None:
            metadata[key] = source[key]
    if source_path:
        metadata["sourcePath"] = str(source_path)

    return ScrapeJob(
        job_id=canonical_job_id(
            kind=kind,
            country_code=country_code,
            source_code=source_code,
        ),
        kind=kind,
        url=url,
        fetcher=fetcher,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        extractor_config=extractor_config,
        schema_ref=str(source.get("schema_ref") or _SCHEMA_REF_BY_KIND[kind]),
        freshness=FreshnessPolicy(max_age_hours=freshness_hours),
        priority=max(0, min(priority, 100)),
        allow_domains=_string_list(source.get("allow_domains")),
        metadata=metadata,
    )


def load_domain_scrape_jobs_from_batch(
    batch_file: str | Path,
    *,
    kind: DomainJobKind,
) -> list[ScrapeJob]:
    """Load policy/incentive/spec source batch YAML as ScrapeJob objects."""
    path = Path(batch_file).expanduser().resolve()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise ValueError(f"{path} did not parse into a YAML mapping")
    batch_code = _require_text(data.get("batch_code"), field_name="batch_code")

    jobs: list[ScrapeJob] = []
    for raw_country in data.get("countries") or []:
        if not isinstance(raw_country, Mapping):
            continue
        country_code = _require_text(
            raw_country.get("country_code"),
            field_name="country_code",
        )
        country_label = _require_text(
            raw_country.get("country_label"),
            field_name="country_label",
        )
        for source in raw_country.get("sources") or []:
            if not isinstance(source, Mapping):
                continue
            jobs.append(
                domain_source_to_scrape_job(
                    source,
                    kind=kind,
                    country_code=country_code,
                    country_label=country_label,
                    batch_code=batch_code,
                    source_path=path,
                )
            )
    return jobs
