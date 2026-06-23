#!/usr/bin/env python3
"""Network-free smoke for News/VOC AI enrichment coverage.

This diagnostic proves that the configured country sources can feed the
heuristic AI enrichment contracts before a real LLM provider or live crawler
run is available for every market.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
if str(TOOLKIT_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLKIT_ROOT))
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
if str(HERMES_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_SCRIPT_DIR))

from jato_scraper.news_config_loader import load_news_batch_config  # noqa: E402
from jato_scraper.news_enricher import build_news_enrichment  # noqa: E402
from jato_scraper.voc_config_loader import load_voc_batch_config  # noqa: E402
from jato_scraper.voc_enricher import (  # noqa: E402
    build_country_voc_enrichment,
    build_voc_enriched_collection,
)

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - optional for direct unit import.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "ai_intelligence_enrichment_smoke_v1"
DEFAULT_NEWS_BATCH = "07_ScrapingToolkit/news_sources/batch_a.yaml"
DEFAULT_VOC_BATCH = "07_ScrapingToolkit/voc_sources/batch_a.yaml"
DEFAULT_REQUIRED_COUNTRIES = (
    "se",
    "fi",
    "no",
    "dk",
    "at",
    "cz",
    "hu",
    "hr",
    "de",
    "fr",
    "it",
    "pl",
    "sk",
    "si",
    "ch",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _safe_token(value: str | None, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    safe = "".join(ch if ch.isalnum() else "-" for ch in text)
    return "-".join(part for part in safe.split("-") if part) or fallback


def _history_suffix(report: dict[str, Any]) -> str:
    generated = str(report.get("generatedAtUtc") or _utc_now_iso())
    stamp = generated.replace(":", "").replace("-", "").replace("+", "z").replace(".", "-")
    return _safe_token(stamp, "unknown-time")


def _resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return repo_root / path


def _normalized_countries(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    countries: list[str] = []
    for value in values:
        code = str(value or "").strip().lower()
        if not code or code in seen:
            continue
        seen.add(code)
        countries.append(code)
    return tuple(countries)


def _required_country_set(values: Sequence[str]) -> set[str]:
    return {country.upper() for country in _normalized_countries(values)}


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    news = summary.get("news") if isinstance(summary.get("news"), dict) else {}
    voc = summary.get("voc") if isinstance(summary.get("voc"), dict) else {}
    warnings = [str(item) for item in report.get("warnings") or [] if str(item).strip()]
    voc_missing_count = (
        len(voc.get("missingEvidenceCountries") or [])
        + len(voc.get("missingPainPointCountries") or [])
        + len(voc.get("missingSentimentCountries") or [])
    )
    lines: list[str] = [
        "# AI News & VOC Enrichment Smoke",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        f"**Required countries:** {summary.get('requiredCountryCount', 0)}",
        "",
        "## Summary",
        "",
        "| Domain | Countries | Records | Signals | Missing |",
        "|---|---:|---:|---:|---|",
        (
            f"| News | {news.get('countryCount', 0)} | "
            f"{news.get('marketEventCount', 0)} | - | "
            f"{len(news.get('missingDigestCountries') or []) + len(news.get('missingEvidenceCountries') or [])} |"
        ),
        (
            f"| VOC | {voc.get('countryCount', 0)} | "
            f"{voc.get('documentCount', 0)} | "
            f"{voc.get('signalObservationCount', 0)} | "
            f"{voc_missing_count} |"
        ),
        "",
        "## Countries",
        "",
        "| Country | News events | Weekly digest | VOC docs | VOC evidence cards |",
        "|---|---:|---|---:|---:|",
    ]
    news_by_country = {
        str(item.get("countryCode") or ""): item
        for item in report.get("countries", {}).get("news", [])
        if isinstance(item, dict)
    }
    voc_by_country = {
        str(item.get("countryCode") or ""): item
        for item in report.get("countries", {}).get("voc", [])
        if isinstance(item, dict)
    }
    for country_code in sorted(set(news_by_country) | set(voc_by_country)):
        news_item = news_by_country.get(country_code, {})
        voc_item = voc_by_country.get(country_code, {})
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(country_code),
                    _markdown_cell(news_item.get("marketEventCount", 0)),
                    _markdown_cell("yes" if news_item.get("weeklyDigestReady") else "no"),
                    _markdown_cell(voc_item.get("documentCount", 0)),
                    _markdown_cell(voc_item.get("evidenceCardCount", 0)),
                ]
            )
            + " |"
        )
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {_markdown_cell(item)}" for item in warnings)
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "ai_intelligence_enrichment_smoke.json"
    latest_md = output_root / "ai_intelligence_enrichment_smoke.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"ai_intelligence_enrichment_smoke_{suffix}.json"
    hist_md = output_root / f"ai_intelligence_enrichment_smoke_{suffix}.md"
    json_text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    md_text = _render_markdown(report)
    latest_json.write_text(json_text, encoding="utf-8")
    latest_md.write_text(md_text, encoding="utf-8")
    hist_json.write_text(json_text, encoding="utf-8")
    hist_md.write_text(md_text, encoding="utf-8")
    return {
        "latestJson": _display_path(latest_json),
        "latestMarkdown": _display_path(latest_md),
        "historicalJson": _display_path(hist_json),
        "historicalMarkdown": _display_path(hist_md),
    }


def write_status_record(
    report: dict[str, Any],
    *,
    artifact_refs: Sequence[str],
) -> dict[str, Any] | None:
    if write_pipeline_status is None:
        return None
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    news = summary.get("news") if isinstance(summary.get("news"), dict) else {}
    voc = summary.get("voc") if isinstance(summary.get("voc"), dict) else {}
    status = str(report.get("status") or "failed")
    pipeline_status = "success" if status == "ok" else "degraded" if status == "degraded" else "failed"
    warning_count = len(report.get("warnings") or [])
    records_processed = int(news.get("marketEventCount") or 0) + int(voc.get("documentCount") or 0)
    return write_pipeline_status(
        pipeline_id="ai_intelligence_enrichment_smoke",
        status=pipeline_status,
        started_at=report.get("generatedAtUtc"),
        finished_at=_utc_now_iso(),
        exit_code=0 if status in {"ok", "degraded"} else 1,
        records_processed=records_processed,
        failed_count=0 if status in {"ok", "degraded"} else 1,
        warning_count=warning_count,
        artifact_refs=list(artifact_refs),
        source="ai_intelligence_enrichment_smoke",
        message=(
            f"AI enrichment smoke={status}; "
            f"newsCountries={news.get('countryCount', 0)}, "
            f"vocCountries={voc.get('countryCount', 0)}, "
            f"warnings={warning_count}."
        ),
        extra={
            "smokeStatus": status,
            "requiredCountryCount": summary.get("requiredCountryCount", 0),
            "news": news,
            "voc": voc,
        },
        repo_root=REPO_ROOT,
    )


def _domain_warning_count(report: dict[str, Any], prefixes: Sequence[str]) -> int:
    warnings = [str(item) for item in report.get("warnings") or [] if str(item).strip()]
    return sum(1 for item in warnings if any(item.startswith(prefix) for prefix in prefixes))


def _domain_pipeline_status(country_count: int, warning_count: int) -> str:
    if country_count <= 0:
        return "failed"
    if warning_count > 0:
        return "degraded"
    return "success"


def write_domain_status_records(
    report: dict[str, Any],
    *,
    artifact_refs: Sequence[str],
) -> list[dict[str, Any]]:
    if write_pipeline_status is None:
        return []
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    news = summary.get("news") if isinstance(summary.get("news"), dict) else {}
    voc = summary.get("voc") if isinstance(summary.get("voc"), dict) else {}
    generated_at = str(report.get("generatedAtUtc") or _utc_now_iso())
    smoke_status = str(report.get("status") or "failed")
    required_country_count = summary.get("requiredCountryCount", 0)
    news_warnings = _domain_warning_count(report, ("news_",))
    voc_warnings = _domain_warning_count(report, ("voc_",))
    records: list[dict[str, Any]] = []

    news_country_count = int(news.get("countryCount") or 0)
    news_records = int(news.get("marketEventCount") or news.get("articleCount") or 0)
    records.append(
        write_pipeline_status(
            pipeline_id="country_news_sync",
            status=_domain_pipeline_status(news_country_count, news_warnings),
            last_run_at=generated_at,
            started_at=generated_at,
            finished_at=_utc_now_iso(),
            exit_code=0 if news_country_count > 0 else 1,
            records_processed=news_records,
            failed_count=0 if news_country_count > 0 else 1,
            warning_count=news_warnings,
            artifact_refs=list(artifact_refs),
            source="ai_intelligence_enrichment_smoke",
            message=(
                "Derived News status from AI enrichment smoke; "
                f"newsCountries={news_country_count}, "
                f"marketEvents={news.get('marketEventCount', 0)}, "
                f"warnings={news_warnings}."
            ),
            extra={
                "derivedFrom": "ai_intelligence_enrichment_smoke",
                "smokeStatus": smoke_status,
                "requiredCountryCount": required_country_count,
                "news": news,
            },
            repo_root=REPO_ROOT,
        )
    )

    voc_country_count = int(voc.get("countryCount") or 0)
    voc_records = int(voc.get("documentCount") or 0)
    records.append(
        write_pipeline_status(
            pipeline_id="voc_forum_sync",
            status=_domain_pipeline_status(voc_country_count, voc_warnings),
            last_run_at=generated_at,
            started_at=generated_at,
            finished_at=_utc_now_iso(),
            exit_code=0 if voc_country_count > 0 else 1,
            records_processed=voc_records,
            failed_count=0 if voc_country_count > 0 else 1,
            warning_count=voc_warnings,
            artifact_refs=list(artifact_refs),
            source="ai_intelligence_enrichment_smoke",
            message=(
                "Derived VOC status from AI enrichment smoke; "
                f"vocCountries={voc_country_count}, "
                f"documents={voc.get('documentCount', 0)}, "
                f"warnings={voc_warnings}."
            ),
            extra={
                "derivedFrom": "ai_intelligence_enrichment_smoke",
                "smokeStatus": smoke_status,
                "requiredCountryCount": required_country_count,
                "voc": voc,
            },
            repo_root=REPO_ROOT,
        )
    )

    return records


def _news_fixture_article(
    *,
    country_code: str,
    country_label: str,
    source_code: str,
    publisher: str,
    language: str,
) -> dict[str, Any]:
    code = country_code.upper()
    return {
        "source_code": source_code,
        "country_code": code,
        "country_label": country_label,
        "publisher": publisher,
        "title": (f"{code} EV incentive and MSRP pricing update affects Tesla Model Y"),
        "url": f"https://news.example/{code.lower()}/ev-pricing-policy",
        "summary": (
            "Government policy, subsidy eligibility, charging network expansion, "
            "and lower MSRP are reshaping BEV demand and SUV competition."
        ),
        "published_at": "2026-06-10T08:00:00+00:00",
        "tags": ["market", "policy", "pricing", "automotive"],
        "raw_payload": {"language": language},
    }


def _build_news_raw_payload(
    *,
    news_batch_path: Path,
    required_countries: Sequence[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    batch = load_news_batch_config(news_batch_path)
    required = _required_country_set(required_countries)
    warnings: list[str] = []
    countries_payload: list[dict[str, Any]] = []
    for country in batch.countries:
        country_code = country.country_code.strip().upper()
        if country_code not in required:
            continue
        if not country.feeds:
            warnings.append(f"news_source_missing:{country_code.lower()}")
            continue
        primary_feed = country.feeds[0]
        article = _news_fixture_article(
            country_code=country_code,
            country_label=country.country_label,
            source_code=primary_feed.source_code,
            publisher=primary_feed.publisher,
            language=primary_feed.language,
        )
        countries_payload.append(
            {
                "country_code": country_code,
                "country_label": country.country_label,
                "source_count": len(country.feeds),
                "article_count": 1,
                "articles": [article],
            }
        )
    observed = {country["country_code"] for country in countries_payload}
    for country in sorted(required - observed):
        warnings.append(f"news_country_missing:{country.lower()}")
    return (
        [
            {
                "batch_code": batch.batch_code,
                "description": batch.description,
                "country_count": len(countries_payload),
                "article_count": len(countries_payload),
                "countries": countries_payload,
                "errors": [],
            }
        ],
        warnings,
    )


def _write_voc_country_fixture(
    *,
    output_root: Path,
    country_code: str,
    country_label: str,
    source_code: str,
    site_name: str,
    site_type: str,
    language: str,
    taxonomy_profile: str,
) -> Path:
    country_root = output_root / country_code.lower()
    raw_root = country_root / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    path = raw_root / f"{country_code.lower()}_ai_fixture.json"
    payload = {
        "source": {
            "source_code": source_code,
            "country_code": country_code.upper(),
            "country_label": country_label,
            "site_name": site_name,
            "site_type": site_type,
            "language": language,
        },
        "taxonomyProfile": taxonomy_profile,
        "collectedAt": "2026-06-11T10:00:00+00:00",
        "autoReview": {
            "publishReadyCount": 2,
            "publishTier": "high",
            "publishDecision": "auto_publish",
        },
        "documentCount": 2,
        "documents": [
            {
                "sourceCode": source_code,
                "countryCode": country_code.upper(),
                "countryLabel": country_label,
                "siteName": site_name,
                "siteType": site_type,
                "language": language,
                "url": f"https://voc.example/{country_code.lower()}/thread-1",
                "title": "Winter range, charging queue, and lease value",
                "publishedAt": "2026-06-10T08:00:00Z",
                "summary": (
                    "Owners discuss winter range loss, public charging reliability, "
                    "software bugs, service wait time, and monthly lease cost."
                ),
                "excerpt": "Winter range and price value decide the shortlist.",
                "rawText": (
                    "After a winter commute, our Tesla Model Y showed range loss "
                    "and the public charging queue was painful. The software update "
                    "fixed one bug, but service wait time and monthly lease price "
                    "still feel expensive compared with Volvo EX30."
                ),
                "replyPosts": [
                    {
                        "unitId": f"{country_code.lower()}-reply-1",
                        "unitType": "reply_post",
                        "author": "fixture_owner",
                        "publishedAt": "2026-06-10T09:00:00Z",
                        "text": (
                            "Charging reliability and winter range matter more "
                            "than premium trim when the lease cost is high."
                        ),
                    }
                ],
                "collectedAt": "2026-06-11T10:00:00+00:00",
                "autoReview": {
                    "score": 8,
                    "publishTier": "high",
                    "publishDecision": "auto_publish",
                },
            },
            {
                "sourceCode": source_code,
                "countryCode": country_code.upper(),
                "countryLabel": country_label,
                "siteName": site_name,
                "siteType": site_type,
                "language": language,
                "url": f"https://voc.example/{country_code.lower()}/thread-2",
                "title": "Family SUV test drive compares BEV and PHEV offers",
                "publishedAt": "2026-06-10T11:00:00Z",
                "summary": "Family buyers compare space, towing, price, and charging.",
                "excerpt": "Price value, range, and family space drive purchase impact.",
                "rawText": (
                    "We are cross-shopping Skoda Enyaq, Volkswagen ID.4, and a "
                    "new PHEV lease. Family space, trailer use, public charging, "
                    "and overall price value matter most."
                ),
                "collectedAt": "2026-06-11T10:00:00+00:00",
                "autoReview": {
                    "score": 7,
                    "publishTier": "high",
                    "publishDecision": "auto_publish",
                },
            },
        ],
        "errors": [],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return country_root


def _build_voc_fixture_artifacts(
    *,
    voc_batch_path: Path,
    output_root: Path,
    required_countries: Sequence[str],
) -> tuple[list[Path], list[str]]:
    batch = load_voc_batch_config(voc_batch_path)
    required = _required_country_set(required_countries)
    warnings: list[str] = []
    country_roots: list[Path] = []
    for country in batch.countries:
        country_code = country.country_code.strip().upper()
        if country_code not in required:
            continue
        if not country.sources:
            warnings.append(f"voc_source_missing:{country_code.lower()}")
            continue
        primary_source = country.sources[0]
        country_roots.append(
            _write_voc_country_fixture(
                output_root=output_root,
                country_code=country_code,
                country_label=country.country_label,
                source_code=primary_source.source_code,
                site_name=primary_source.site_name,
                site_type=primary_source.site_type,
                language=primary_source.language,
                taxonomy_profile=country.taxonomy_profile,
            )
        )
    observed = {path.name.upper() for path in country_roots}
    for country in sorted(required - observed):
        warnings.append(f"voc_country_missing:{country.lower()}")
    return country_roots, warnings


def _news_summary(enrichment: dict[str, Any]) -> dict[str, Any]:
    countries = [country for country in enrichment.get("countries") or [] if isinstance(country, dict)]
    missing_digest = [
        str(country.get("countryCode")).lower() for country in countries if not country.get("weeklyDigest")
    ]
    missing_evidence = [
        str(country.get("countryCode")).lower()
        for country in countries
        if not any(
            (event.get("evidenceCard") if isinstance(event, dict) else None)
            for event in country.get("marketEvents") or []
        )
    ]
    event_types = Counter()
    sentiments = Counter()
    for country in countries:
        for event in country.get("marketEvents") or []:
            if not isinstance(event, dict):
                continue
            event_types.update([str(event.get("eventType") or "unknown")])
            sentiments.update([str(event.get("sentiment") or "unknown")])
    return {
        "countryCount": len(countries),
        "articleCount": enrichment.get("articleCount", 0),
        "marketEventCount": enrichment.get("marketEventCount", 0),
        "eventTypeCounts": dict(sorted(event_types.items())),
        "sentimentCounts": dict(sorted(sentiments.items())),
        "missingDigestCountries": missing_digest,
        "missingEvidenceCountries": missing_evidence,
    }


def _voc_summary(country_roots: Sequence[Path]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    country_payloads: list[dict[str, Any]] = []
    missing_evidence: list[str] = []
    missing_pain_points: list[str] = []
    missing_sentiment: list[str] = []
    total_documents = 0
    total_observations = 0
    sentiment_counts = Counter()
    pain_point_counts = Counter()

    for country_root in country_roots:
        enrichment = build_country_voc_enrichment(country_root)
        country_code = str(enrichment.get("countryCode") or country_root.name.upper())
        documents = [item for item in enrichment.get("documents") or [] if isinstance(item, dict)]
        total_documents += int(enrichment.get("documentCount") or len(documents))
        total_observations += int(enrichment.get("signalObservationCount") or 0)
        aggregates = enrichment.get("aggregates")
        aggregate_payload = aggregates if isinstance(aggregates, dict) else {}
        evidence_cards = aggregate_payload.get("evidenceCards") or []
        pain_points = aggregate_payload.get("painPoints") or []
        if not evidence_cards:
            missing_evidence.append(country_code.lower())
        if not pain_points:
            missing_pain_points.append(country_code.lower())
        if not any(document.get("sentiment") for document in documents):
            missing_sentiment.append(country_code.lower())
        for document in documents:
            sentiment_counts.update([str(document.get("sentiment") or "unknown")])
        for item in pain_points:
            if isinstance(item, dict):
                pain_point_counts.update([str(item.get("key") or item.get("label") or "unknown")])
        country_payloads.append(
            {
                "countryCode": country_code,
                "documentCount": enrichment.get("documentCount", len(documents)),
                "publishReadyDocumentCount": enrichment.get(
                    "publishReadyDocumentCount",
                    0,
                ),
                "signalObservationCount": enrichment.get("signalObservationCount", 0),
                "evidenceCardCount": len(evidence_cards),
                "painPointCount": len(pain_points),
            }
        )

    return (
        {
            "countryCount": len(country_payloads),
            "documentCount": total_documents,
            "signalObservationCount": total_observations,
            "sentimentCounts": dict(sorted(sentiment_counts.items())),
            "painPointCounts": dict(sorted(pain_point_counts.items())),
            "missingEvidenceCountries": missing_evidence,
            "missingPainPointCountries": missing_pain_points,
            "missingSentimentCountries": missing_sentiment,
        },
        country_payloads,
    )


def run_smoke(
    *,
    repo_root: str | Path | None = None,
    news_batch: str | Path = DEFAULT_NEWS_BATCH,
    voc_batch: str | Path = DEFAULT_VOC_BATCH,
    artifact_root: str | Path | None = None,
    required_countries: Sequence[str] = DEFAULT_REQUIRED_COUNTRIES,
    write_voc_collection: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve() if repo_root else REPO_ROOT
    output_root = (
        Path(artifact_root).expanduser().resolve()
        if artifact_root is not None
        else Path(tempfile.mkdtemp(prefix="jato_ai_intel_smoke_"))
    )
    output_root.mkdir(parents=True, exist_ok=True)
    required = _normalized_countries(required_countries)
    news_batch_path = _resolve_path(root, news_batch)
    voc_batch_path = _resolve_path(root, voc_batch)

    news_raw, news_warnings = _build_news_raw_payload(
        news_batch_path=news_batch_path,
        required_countries=required,
    )
    generated_at = _utc_now_iso()
    news_enrichment = build_news_enrichment(
        news_raw,
        required_countries=required,
        generated_at_utc=generated_at,
    )
    news_output = output_root / "news_ai_enriched_fixture.json"
    news_output.write_text(
        json.dumps(news_enrichment, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    voc_root = output_root / "voc"
    country_roots, voc_warnings = _build_voc_fixture_artifacts(
        voc_batch_path=voc_batch_path,
        output_root=voc_root,
        required_countries=required,
    )
    voc_summary, voc_countries = _voc_summary(country_roots)
    voc_collection_summary = (
        build_voc_enriched_collection(
            output_root=voc_root,
            country_filter={country.upper() for country in required},
        )
        if write_voc_collection
        else None
    )

    news_status = _news_summary(news_enrichment)
    warnings = [
        *news_warnings,
        *voc_warnings,
        *list(news_enrichment.get("warnings") or []),
        *[f"news_missing_digest:{country}" for country in news_status["missingDigestCountries"]],
        *[f"news_missing_evidence:{country}" for country in news_status["missingEvidenceCountries"]],
        *[f"voc_missing_evidence:{country}" for country in voc_summary["missingEvidenceCountries"]],
        *[f"voc_missing_pain_points:{country}" for country in voc_summary["missingPainPointCountries"]],
        *[f"voc_missing_sentiment:{country}" for country in voc_summary["missingSentimentCountries"]],
    ]
    status = "ok"
    if news_status["countryCount"] == 0 and voc_summary["countryCount"] == 0:
        status = "failed"
    elif warnings:
        status = "degraded"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": status,
        "generatedAtUtc": generated_at,
        "artifactRoot": str(output_root),
        "inputs": {
            "newsBatch": str(news_batch_path),
            "vocBatch": str(voc_batch_path),
            "requiredCountries": list(required),
        },
        "summary": {
            "requiredCountryCount": len(required),
            "news": news_status,
            "voc": voc_summary,
        },
        "artifacts": {
            "newsEnrichment": str(news_output),
            "vocRoot": str(voc_root),
            "vocCollectionSummary": voc_collection_summary,
        },
        "countries": {
            "news": [
                {
                    "countryCode": country.get("countryCode"),
                    "marketEventCount": country.get("marketEventCount"),
                    "weeklyDigestReady": bool(country.get("weeklyDigest")),
                    "eventTypeCounts": country.get("eventTypeCounts"),
                }
                for country in news_enrichment.get("countries") or []
                if isinstance(country, dict)
            ],
            "voc": voc_countries,
        },
        "warnings": warnings,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a network-free News/VOC AI enrichment smoke.",
    )
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--news-batch", default=DEFAULT_NEWS_BATCH)
    parser.add_argument("--voc-batch", default=DEFAULT_VOC_BATCH)
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional Hermes reports directory for JSON/Markdown smoke report.",
    )
    parser.add_argument(
        "--write-status",
        action="store_true",
        help="Write hermes/reports/pipeline_status/ai_intelligence_enrichment_smoke.json.",
    )
    parser.add_argument(
        "--required-countries",
        default=",".join(DEFAULT_REQUIRED_COUNTRIES),
        help="Comma-separated country codes to verify.",
    )
    parser.add_argument(
        "--no-voc-collection",
        action="store_true",
        help="Skip writing enriched/deck VOC collection artifacts.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless the smoke status is ok.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    countries = [part.strip() for part in str(args.required_countries).split(",") if part.strip()]
    report = run_smoke(
        repo_root=args.repo_root,
        news_batch=args.news_batch,
        voc_batch=args.voc_batch,
        artifact_root=args.artifact_root,
        required_countries=countries,
        write_voc_collection=not bool(args.no_voc_collection),
    )
    report_artifacts: dict[str, str] = {}
    if args.out_dir:
        report_artifacts = write_outputs(report, args.out_dir)
        report["reportArtifacts"] = report_artifacts
    if args.write_status:
        artifact_refs = [
            *list(report_artifacts.values()),
            str(report.get("artifacts", {}).get("newsEnrichment") or ""),
            str(report.get("artifacts", {}).get("vocRoot") or ""),
        ]
        status_record = write_status_record(
            report,
            artifact_refs=artifact_refs,
        )
        if status_record is not None:
            report["pipelineStatus"] = status_record
        domain_status_records = write_domain_status_records(
            report,
            artifact_refs=artifact_refs,
        )
        if domain_status_records:
            report["pipelineStatuses"] = domain_status_records
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] == "failed":
        return 1
    if args.strict and report["status"] != "ok":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
