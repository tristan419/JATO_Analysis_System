"""Local fixture pipeline for exercising unified scraping stages.

This module is deliberately network-free. It lets diagnostics and tests verify
that every ScrapeJob kind can move through Fetcher -> Extractor -> Normalizer
-> Sink contracts before a real fetcher or database sink exists.
"""

from __future__ import annotations

from pathlib import Path
import json
import re
from typing import Any, Iterable

from jato_scraper.core.job import (
    FetchMetadata,
    NormalizedObservation,
    ObservationQuality,
    RawDocument,
    ScrapeJob,
    ScrapeRunLog,
    SinkResult,
    StructuredObservation,
)


_NON_KEY_RE = re.compile(r"[^a-z0-9]+")


def _string(value: object | None, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _string_list(value: object | None) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_items = [value]
    else:
        raw_items = list(value)  # type: ignore[arg-type]
    return [
        str(item).strip()
        for item in raw_items
        if str(item).strip()
    ]


def _key_part(value: object | None, default: str = "unknown") -> str:
    candidate = _string(value, default).lower()
    normalized = _NON_KEY_RE.sub("_", candidate).strip("_")
    return normalized or default


def _preview(text: str, limit: int = 240) -> str:
    normalized = " ".join(text.split())
    return normalized[:limit]


def _fixture_body(job: ScrapeJob) -> str:
    country = _string(job.metadata.get("countryCode"), "xx").upper()
    source_name = _string(
        job.metadata.get("sourceName") or job.metadata.get("publisher"),
        job.metadata.get("sourceCode") or job.job_id,
    )
    topics = ", ".join(_string_list(job.metadata.get("topics"))) or "pricing"
    if job.kind == "policy":
        return (
            f"{country} policy source {source_name} covers {topics}. "
            "The document explains vehicle taxation, registration rules, and "
            "effective dates for automotive market monitoring."
        )
    if job.kind == "incentive":
        applies_to = ", ".join(_string_list(job.metadata.get("applies_to"))) or "BEV"
        return (
            f"{country} incentive source {source_name} covers {topics}. "
            f"The program applies to {applies_to} vehicles and describes "
            "eligibility, value caps, and consumer price impact."
        )
    if job.kind == "spec":
        brand = _string(job.metadata.get("brand"), "BRAND")
        model = _string(job.metadata.get("model"), "MODEL")
        return (
            f"{country} official specification page for {brand} {model}. "
            "The page lists trim features, powertrain availability, equipment, "
            "and configuration options."
        )
    if job.kind == "news":
        return (
            f"{country} automotive market news from {source_name}. "
            "The article discusses EV demand, pricing changes, policy updates, "
            "and expected market impact."
        )
    if job.kind == "voc":
        return (
            f"{country} public owner discussion from {source_name}. "
            "Owners discuss winter range, charging reliability, software, "
            "service wait times, and price value."
        )
    return (
        f"{country} official MSRP source {source_name}. "
        "The page contains trim price text, model labels, source currency, "
        "and match evidence for current price materialization."
    )


def fetch_fixture_raw_document(
    job: ScrapeJob,
    *,
    body_text: str | None = None,
) -> RawDocument:
    """Build a RawDocument for a job without performing network IO."""
    body = body_text if body_text is not None else _fixture_body(job)
    return RawDocument(
        job_id=job.job_id,
        content_type="text",
        body_text=body,
        metadata=FetchMetadata(
            final_url=job.url,
            status_code=200,
            headers={"x-fixture-fetcher": job.fetcher},
        ),
    )


def extract_fixture_structured_observation(
    job: ScrapeJob,
    document: RawDocument,
) -> StructuredObservation:
    """Create one schema-shaped StructuredObservation from a fixture document."""
    text = document.body_text or ""
    country_code = _string(job.metadata.get("countryCode"), "xx").lower()
    source_code = _string(job.metadata.get("sourceCode"), job.job_id)
    topics = _string_list(job.metadata.get("topics"))
    source_url = document.metadata.final_url or job.url
    payload: dict[str, Any] = {
        "countryCode": country_code.upper(),
        "sourceCode": source_code,
        "sourceUrl": source_url,
        "textPreview": _preview(text),
        "topics": topics,
    }

    if job.kind == "policy":
        payload.update(
            {
                "topic": topics[0] if topics else "policy",
                "title": _string(job.metadata.get("sourceName"), "Policy source"),
                "bodyMarkdown": text,
                "sourceKind": _string(job.metadata.get("sourceKind"), "unknown"),
                "citationTier": int(job.metadata.get("citation_tier") or 3),
            }
        )
    elif job.kind == "incentive":
        payload.update(
            {
                "programName": _string(
                    job.metadata.get("program_name")
                    or job.metadata.get("sourceName"),
                    "Vehicle incentive program",
                ),
                "appliesTo": _string_list(job.metadata.get("applies_to")),
                "conditions": text,
                "sourceKind": _string(job.metadata.get("sourceKind"), "unknown"),
            }
        )
    elif job.kind == "spec":
        payload.update(
            {
                "brand": _string(job.metadata.get("brand"), "UNKNOWN"),
                "model": _string(job.metadata.get("model"), "UNKNOWN"),
                "featureItems": [
                    {
                        "featureKey": "fixture_standard_equipment",
                        "kind": "standard",
                        "label": "Fixture standard equipment",
                    }
                ],
            }
        )
    elif job.kind == "news":
        payload.update(
            {
                "title": _string(job.metadata.get("publisher"), "Market update"),
                "eventType": "market_update",
                "marketImpact": "monitor",
                "language": job.metadata.get("language"),
            }
        )
    elif job.kind == "voc":
        payload.update(
            {
                "signalKind": "owner_discussion",
                "sentiment": "neutral",
                "painPoints": ["price_value"],
                "evidenceQuote": _preview(text, 160),
            }
        )
    else:
        payload.update(
            {
                "priceSemantics": job.metadata.get("priceSemantics", "base_msrp"),
                "sourceType": job.metadata.get("sourceType"),
                "observedPriceText": _preview(text, 160),
            }
        )

    warnings = [] if len(text.strip()) >= 40 else ["short_fixture_document"]
    return StructuredObservation(
        job_id=job.job_id,
        kind=job.kind,
        schema_ref=job.schema_ref,
        payload=payload,
        source_url=source_url,
        confidence=0.8 if not warnings else 0.45,
        warnings=warnings,
        metadata={
            "fetcher": job.fetcher,
            "extractor": job.extractor,
            "sourceCode": source_code,
            "countryCode": country_code,
        },
    )


def normalize_fixture_observation(
    observation: StructuredObservation,
) -> NormalizedObservation:
    """Normalize a structured fixture observation to a stable record key."""
    payload = dict(observation.payload)
    country_code = _key_part(payload.get("countryCode"))
    source_code = _key_part(payload.get("sourceCode"))
    subject = (
        payload.get("topic")
        or payload.get("programName")
        or payload.get("model")
        or payload.get("eventType")
        or payload.get("signalKind")
        or payload.get("priceSemantics")
        or observation.schema_ref
    )
    record_key = (
        f"{_key_part(observation.kind)}:{country_code}:{source_code}:"
        f"{_key_part(subject)}"
    )[:160]
    source_kind = _string(payload.get("sourceKind")).lower()
    high_quality_kinds = {"government", "manufacturer_official", "association"}
    quality: ObservationQuality = (
        "high"
        if source_kind in high_quality_kinds or observation.kind == "msrp"
        else "medium"
    )
    if observation.confidence is not None and observation.confidence < 0.5:
        quality = "low"

    payload["normalizedCountryCode"] = country_code.upper()
    return NormalizedObservation(
        job_id=observation.job_id,
        kind=observation.kind,
        schema_ref=observation.schema_ref,
        record_key=record_key,
        payload=payload,
        quality=quality,
        source_url=observation.source_url,
        warnings=list(observation.warnings),
        metadata=dict(observation.metadata),
    )


def sink_normalized_observations_to_jsonl(
    observations: Iterable[NormalizedObservation],
    *,
    artifact_root: str | Path,
    sink_name: str = "local_jsonl",
) -> SinkResult:
    """Write normalized observations to a JSONL artifact and return SinkResult."""
    items = list(observations)
    if not items:
        return SinkResult(
            job_id="none",
            kind="news",
            sink_name=sink_name,
            status="skipped",
            rows_in=0,
            rows_written=0,
            rows_skipped=0,
            warnings=["no_observations_to_sink"],
        )

    root = Path(artifact_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    first = items[0]
    destination = root / f"{first.job_id.replace(':', '_')}.jsonl"
    with open(destination, "w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item.model_dump(mode="json"), ensure_ascii=False))
            handle.write("\n")

    return SinkResult(
        job_id=first.job_id,
        kind=first.kind,
        sink_name=sink_name,
        status="ok",
        rows_in=len(items),
        rows_written=len(items),
        rows_skipped=0,
        artifact_refs=[str(destination)],
        metadata={"artifactFormat": "jsonl"},
    )


def run_fixture_pipeline_for_job(
    job: ScrapeJob,
    *,
    artifact_root: str | Path,
    body_text: str | None = None,
) -> dict[str, Any]:
    """Run one job through the local fixture pipeline stages."""
    run_log = ScrapeRunLog(
        job_id=job.job_id,
        kind=job.kind,
        fetcher=job.fetcher,
        extractor=job.extractor,
    )
    raw_document = fetch_fixture_raw_document(job, body_text=body_text)
    structured = extract_fixture_structured_observation(job, raw_document)
    normalized = normalize_fixture_observation(structured)
    sink_result = sink_normalized_observations_to_jsonl(
        [normalized],
        artifact_root=artifact_root,
    )
    finished_log = run_log.finish(
        status="ok" if sink_result.status == "ok" else "skipped",
        rows_out=sink_result.rows_written,
    )
    return {
        "rawDocument": raw_document,
        "structuredObservations": [structured],
        "normalizedObservations": [normalized],
        "sinkResult": sink_result,
        "runLog": finished_log,
    }
