from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError
import pytest

from jato_scraper.core import (
    FetchMetadata,
    FreshnessPolicy,
    NormalizedObservation,
    RawDocument,
    ScrapeJob,
    ScrapeRunLog,
    SinkResult,
    StructuredObservation,
    canonical_job_id,
    domain_source_to_scrape_job,
    load_domain_scrape_jobs_from_batch,
    load_msrp_scrape_jobs_from_dir,
    msrp_source_to_scrape_job,
    news_feed_to_scrape_job,
    voc_source_to_scrape_job,
)
from jato_scraper.news_config_loader import load_news_batch_config
from jato_scraper.voc_config_loader import load_voc_batch_config


def test_scrape_job_normalizes_domains_and_serializes_queue_payload():
    job = ScrapeJob(
        job_id=canonical_job_id(
            kind="msrp",
            country_code="SE",
            source_code="volvo_xc60",
        ),
        kind="msrp",
        url="https://www.volvocars.com/se/build/xc60-hybrid/",
        fetcher="scrapling",
        extractor="css_rules",
        extractor_config={"vehicle_container": "[data-testid='selection-card']"},
        schema_ref="RawMsrpObservation",
        freshness=FreshnessPolicy(max_age_hours=168),
        priority=80,
        allow_domains=[
            "https://www.volvocars.com/se",
            "www.volvocars.com",
        ],
        metadata={"country": "SE", "brand": "VOLVO", "model": "XC60"},
    )

    assert job.job_id == "msrp:se:volvo_xc60"
    assert job.allow_domains == ["www.volvocars.com"]
    assert job.to_queue_payload()["freshness"] == {
        "max_age_hours": 168,
        "skip_if_fresh": True,
    }


def test_scrape_job_defaults_allow_domain_from_url():
    job = ScrapeJob(
        job_id="news:fi:local_media",
        kind="news",
        url="https://example.fi/feed.xml",
        fetcher="httpx",
        extractor="rss",
        schema_ref="RawNewsArticle",
        freshness={"max_age_hours": 24},
    )

    assert job.allow_domains == ["example.fi"]


@pytest.mark.parametrize(
    "payload",
    [
        {"url": "not-a-url"},
        {"priority": 101},
        {"fetcher": "selenium"},
        {"extractor": "unknown"},
    ],
)
def test_scrape_job_rejects_invalid_contract_values(payload):
    base = {
        "job_id": "msrp:se:test",
        "kind": "msrp",
        "url": "https://example.com/source",
        "fetcher": "requests",
        "extractor": "json_path",
        "schema_ref": "RawMsrpObservation",
        "freshness": {"max_age_hours": 12},
    }
    base.update(payload)

    with pytest.raises(ValidationError):
        ScrapeJob(**base)


def test_raw_document_requires_a_body_and_keeps_fetch_metadata():
    doc = RawDocument(
        job_id="news:se:rss",
        content_type="xml",
        body_text="<rss />",
        metadata=FetchMetadata(
            final_url="https://example.se/rss.xml",
            status_code=200,
            headers={"content-type": "application/rss+xml"},
        ),
    )

    assert doc.metadata.status_code == 200
    assert doc.body_text == "<rss />"

    with pytest.raises(ValidationError):
        RawDocument(job_id="news:se:rss", content_type="xml")


def test_structured_and_normalized_observations_capture_pipeline_stages():
    extracted = StructuredObservation(
        job_id="news:se:source",
        kind="news",
        schema_ref="NewsArticle",
        payload={
            "url": "https://example.se/article",
            "title": "Swedish EV demand rises",
        },
        source_url="https://example.se/article",
        confidence=0.86,
    )
    normalized = NormalizedObservation(
        job_id=extracted.job_id,
        kind=extracted.kind,
        schema_ref=extracted.schema_ref,
        record_key="news:se:example:swedish-ev-demand-rises",
        payload={
            **extracted.payload,
            "countryCode": "SE",
            "eventType": "market_update",
        },
        quality="high",
        source_url=extracted.source_url,
    )

    assert extracted.confidence == 0.86
    assert normalized.record_key == "news:se:example:swedish-ev-demand-rises"
    assert normalized.quality == "high"
    assert normalized.payload["countryCode"] == "SE"

    with pytest.raises(ValidationError):
        StructuredObservation(
            job_id="news:se:source",
            kind="news",
            schema_ref="NewsArticle",
            payload={},
        )
    with pytest.raises(ValidationError):
        NormalizedObservation(
            job_id="news:se:source",
            kind="news",
            schema_ref="NewsArticle",
            record_key=" ",
            payload={"title": "Missing key"},
        )


def test_sink_result_validates_stage_row_counts():
    result = SinkResult(
        job_id="msrp:dk:source",
        kind="msrp",
        sink_name="msrp.observations",
        status="ok",
        rows_in=3,
        rows_written=2,
        rows_skipped=1,
        artifact_refs=["03_Scripts/diagnostics/artifacts/dryrun_report.json"],
    )

    assert result.rows_written == 2
    assert result.artifact_refs == [
        "03_Scripts/diagnostics/artifacts/dryrun_report.json"
    ]

    with pytest.raises(ValidationError):
        SinkResult(
            job_id="msrp:dk:source",
            kind="msrp",
            sink_name="msrp.observations",
            status="ok",
            rows_in=1,
            rows_written=1,
            rows_skipped=1,
        )


def test_scrape_run_log_finish_is_immutable_update():
    running = ScrapeRunLog(
        job_id="voc:dk:forum",
        kind="news",
        fetcher="httpx",
        extractor="rss",
        http_status=200,
        bytes_fetched=1024,
    )

    finished = running.finish(status="ok", rows_out=3)

    assert running.status == "running"
    assert finished.status == "ok"
    assert finished.rows_out == 3
    assert finished.finished_at is not None
    assert finished.duration_seconds is not None


def test_msrp_pdf_source_maps_to_unified_scrape_job():
    job = msrp_source_to_scrape_job(
        {
            "source_code": "ford_explorer_ev_fi_draft_scrapling",
            "country": "芬兰",
            "brand": "FORD",
            "source_url": "https://www.ford.fi/henkiloautot/electric-explorer/pricelist.html",
            "source_type": "official_price_list",
            "price_semantics": "base_msrp",
            "extractor_type": "pdf_text",
            "profile": {
                "url": "https://www.ford.fi/content/dam/guxeu/fi/documents/pricelists/cars/electric-explorer/PL.pdf",
                "fixed_model": "EXPLORER EV",
            },
            "schedule": {"frequency": "weekly"},
        },
        source_path="source_drafts/fi/23_ford_explorer_ev_fi.yaml",
    )

    assert job.job_id == "msrp:fi:ford_explorer_ev_fi_draft_scrapling"
    assert job.kind == "msrp"
    assert job.fetcher == "requests"
    assert job.extractor == "pdf_text"
    assert job.url.endswith("/PL.pdf")
    assert job.allow_domains == ["www.ford.fi"]
    assert job.freshness.max_age_hours == 168
    assert job.metadata["sourceUrl"].endswith("/pricelist.html")
    assert job.metadata["countryCode"] == "fi"
    assert job.extractor_config["extractorType"] == "pdf_text"


def test_msrp_scrapling_requests_tier_maps_to_requests_fetcher():
    job = msrp_source_to_scrape_job(
        {
            "source_code": "nissan_qashqai_fi_draft_scrapling",
            "country": "芬兰",
            "brand": "NISSAN",
            "source_url": "https://www.nissan.fi/ajoneuvot/henkiloautot/qashqai.html",
            "extractor_type": "scrapling",
            "profile": {
                "url": "https://www.nissan.fi/ajoneuvot/henkiloautot/qashqai.html",
                "tier": "requests",
                "css": {"vehicle_container": ".gradewalk-card"},
            },
            "schedule": {"frequency": "daily"},
        },
    )

    assert job.fetcher == "requests"
    assert job.extractor == "scrapling"
    assert job.freshness.max_age_hours == 24


def test_load_msrp_scrape_jobs_from_dir(tmp_path):
    source_file = tmp_path / "fi" / "23_ford_explorer_ev_fi.yaml"
    source_file.parent.mkdir()
    preset_file = tmp_path / "_shared" / "presets" / "brand_preset.yaml"
    preset_file.parent.mkdir(parents=True)
    preset_file.write_text(
        """
profile:
  tier: dynamic
""",
        encoding="utf-8",
    )
    source_file.write_text(
        """
source_code: ford_explorer_ev_fi_draft_scrapling
country: 芬兰
brand: FORD
source_url: https://www.ford.fi/henkiloautot/electric-explorer/pricelist.html
extractor_type: pdf_text
profile:
  url: https://www.ford.fi/content/dam/guxeu/fi/documents/pricelists/cars/electric-explorer/PL.pdf
""",
        encoding="utf-8",
    )

    jobs = load_msrp_scrape_jobs_from_dir(tmp_path)

    assert len(jobs) == 1
    assert jobs[0].metadata["sourcePath"].endswith("23_ford_explorer_ev_fi.yaml")


def test_news_feed_maps_to_unified_scrape_job():
    batch = load_news_batch_config(
        Path(__file__).resolve().parents[1] / "news_sources" / "batch_a.yaml"
    )
    feed = batch.countries[0].feeds[0]

    job = news_feed_to_scrape_job(feed)

    assert job.job_id == "news:se:se_google_auto_market"
    assert job.kind == "news"
    assert job.fetcher == "requests"
    assert job.extractor == "rss"
    assert job.schema_ref == "NewsArticle"
    assert job.freshness.max_age_hours == 24
    assert job.metadata["publisher"] == "Google News"
    assert job.metadata["countryCode"] == "se"


def test_voc_source_maps_to_unified_scrape_job():
    batch = load_voc_batch_config(
        Path(__file__).resolve().parents[1] / "voc_sources" / "batch_a.yaml"
    )
    source = batch.countries[0].sources[0]

    job = voc_source_to_scrape_job(source)

    assert job.job_id == "voc:se:se_teslaclubsweden_forum"
    assert job.kind == "voc"
    assert job.fetcher == "scrapling"
    assert job.extractor == "scrapling"
    assert job.schema_ref == "VocRawDocument"
    assert job.freshness.max_age_hours == 168
    assert job.metadata["siteName"] == "Tesla Club Sweden Forum"
    assert job.metadata["publicAccess"] is True


def test_policy_source_maps_to_unified_scrape_job():
    job = domain_source_to_scrape_job(
        {
            "source_code": "se_transport_agency_policy",
            "source_name": "Swedish Transport Agency policy notices",
            "url": "https://www.transportstyrelsen.se/en/road/Vehicles/",
            "source_kind": "government",
            "topics": ["registration", "emissions"],
            "citation_tier": 1,
        },
        kind="policy",
        country_code="SE",
        country_label="Sweden / 瑞典",
        batch_code="policy_batch_a",
        source_path="policy_sources/batch_a.yaml",
    )

    assert job.job_id == "policy:se:se_transport_agency_policy"
    assert job.kind == "policy"
    assert job.fetcher == "requests"
    assert job.extractor == "llm_extract"
    assert job.schema_ref == "PolicySnippet"
    assert job.freshness.max_age_hours == 720
    assert job.metadata["sourceKind"] == "government"
    assert job.metadata["topics"] == ["registration", "emissions"]
    assert job.metadata["citation_tier"] == 1


def test_incentive_and_spec_batches_load_to_unified_scrape_jobs():
    root = Path(__file__).resolve().parents[1]
    incentive_jobs = load_domain_scrape_jobs_from_batch(
        root / "incentive_sources" / "batch_a.yaml",
        kind="incentive",
    )
    spec_jobs = load_domain_scrape_jobs_from_batch(
        root / "spec_sources" / "batch_a.yaml",
        kind="spec",
    )

    assert {job.kind for job in incentive_jobs} == {"incentive"}
    assert {job.kind for job in spec_jobs} == {"spec"}
    assert incentive_jobs[0].schema_ref == "IncentiveProgram"
    assert incentive_jobs[0].metadata["applies_to"]
    assert spec_jobs[0].schema_ref == "SpecFeatureObservation"
    assert spec_jobs[0].fetcher == "scrapling"
    assert spec_jobs[0].extractor == "css_rules"
    assert spec_jobs[0].metadata["brand"]
