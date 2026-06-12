"""Shared contracts for the unified scraping pipeline."""

from jato_scraper.core.job import (
    ExtractorKind,
    FetchMetadata,
    FetcherKind,
    FreshnessPolicy,
    JobKind,
    NormalizedObservation,
    ObservationQuality,
    RawDocument,
    ScrapeJob,
    ScrapeRunLog,
    ScrapeRunStatus,
    SinkResult,
    StructuredObservation,
    canonical_job_id,
)
from jato_scraper.core.local_pipeline import (
    extract_fixture_structured_observation,
    fetch_fixture_raw_document,
    normalize_fixture_observation,
    run_fixture_pipeline_for_job,
    sink_normalized_observations_to_jsonl,
)
from jato_scraper.core.intelligence_adapter import (
    news_feed_to_scrape_job,
    voc_source_to_scrape_job,
)
from jato_scraper.core.domain_adapter import (
    domain_source_to_scrape_job,
    load_domain_scrape_jobs_from_batch,
)
from jato_scraper.core.msrp_adapter import (
    load_msrp_scrape_jobs_from_dir,
    msrp_source_to_scrape_job,
)

__all__ = [
    "ExtractorKind",
    "FetchMetadata",
    "FetcherKind",
    "FreshnessPolicy",
    "JobKind",
    "NormalizedObservation",
    "ObservationQuality",
    "RawDocument",
    "ScrapeJob",
    "ScrapeRunLog",
    "ScrapeRunStatus",
    "SinkResult",
    "StructuredObservation",
    "canonical_job_id",
    "domain_source_to_scrape_job",
    "load_domain_scrape_jobs_from_batch",
    "load_msrp_scrape_jobs_from_dir",
    "msrp_source_to_scrape_job",
    "news_feed_to_scrape_job",
    "extract_fixture_structured_observation",
    "fetch_fixture_raw_document",
    "normalize_fixture_observation",
    "run_fixture_pipeline_for_job",
    "sink_normalized_observations_to_jsonl",
    "voc_source_to_scrape_job",
]
