from __future__ import annotations

import json
from pathlib import Path

from jato_scraper.core import (
    domain_source_to_scrape_job,
    fetch_fixture_raw_document,
    normalize_fixture_observation,
    run_fixture_pipeline_for_job,
    extract_fixture_structured_observation,
)


def _policy_job():
    return domain_source_to_scrape_job(
        {
            "source_code": "se_policy_source",
            "source_name": "Swedish Vehicle Policy",
            "url": "https://policy.example.se/source",
            "source_kind": "government",
            "topics": ["vehicle_tax"],
            "citation_tier": 1,
        },
        kind="policy",
        country_code="SE",
        country_label="Sweden / 瑞典",
        batch_code="policy_batch_a",
    )


def test_fixture_pipeline_runs_policy_job_through_all_stages(tmp_path: Path) -> None:
    job = _policy_job()

    result = run_fixture_pipeline_for_job(job, artifact_root=tmp_path)

    raw = result["rawDocument"]
    structured = result["structuredObservations"][0]
    normalized = result["normalizedObservations"][0]
    sink = result["sinkResult"]
    run_log = result["runLog"]

    assert raw.job_id == job.job_id
    assert raw.metadata.status_code == 200
    assert structured.kind == "policy"
    assert structured.payload["topic"] == "vehicle_tax"
    assert normalized.record_key.startswith("policy:se:se_policy_source")
    assert normalized.quality == "high"
    assert sink.status == "ok"
    assert sink.rows_written == 1
    assert run_log.status == "ok"
    artifact_path = Path(sink.artifact_refs[0])
    assert artifact_path.exists()
    row = json.loads(artifact_path.read_text(encoding="utf-8").splitlines()[0])
    assert row["record_key"] == normalized.record_key


def test_fixture_extract_and_normalize_spec_job(tmp_path: Path) -> None:
    job = domain_source_to_scrape_job(
        {
            "source_code": "dk_skoda_enyaq_specs",
            "source_name": "Skoda Denmark Enyaq specs",
            "url": "https://spec.example.dk/enyaq",
            "source_kind": "manufacturer_official",
            "brand": "SKODA",
            "model": "ENYAQ",
            "topics": ["trim_features"],
            "fetcher": "scrapling",
            "extractor": "css_rules",
        },
        kind="spec",
        country_code="DK",
        country_label="Denmark / 丹麦",
    )

    raw = fetch_fixture_raw_document(job)
    structured = extract_fixture_structured_observation(job, raw)
    normalized = normalize_fixture_observation(structured)

    assert structured.payload["brand"] == "SKODA"
    assert structured.payload["featureItems"][0]["featureKey"]
    assert normalized.kind == "spec"
    assert normalized.record_key == "spec:dk:dk_skoda_enyaq_specs:enyaq"
    assert normalized.payload["normalizedCountryCode"] == "DK"
