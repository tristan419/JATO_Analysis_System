#!/usr/bin/env python3
"""Read-only audit for the unified scraping pipeline contracts."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import yaml

from jato_scraper.core import (
    ScrapeJob,
    load_domain_scrape_jobs_from_batch,
    msrp_source_to_scrape_job,
    news_feed_to_scrape_job,
    voc_source_to_scrape_job,
)
from jato_scraper.news_config_loader import load_news_batch_config
from jato_scraper.voc_config_loader import load_voc_batch_config


SCHEMA_VERSION = "unified_scraping_contract_audit_v1"
DEFAULT_REQUIRED_COUNTRIES = ("se", "fi", "no", "dk")
DEFAULT_MSRP_DIR = "07_ScrapingToolkit/source_drafts/suv_only_country_model_top30"
DEFAULT_NEWS_BATCH = "07_ScrapingToolkit/news_sources/batch_a.yaml"
DEFAULT_VOC_BATCH = "07_ScrapingToolkit/voc_sources/batch_a.yaml"
DEFAULT_POLICY_BATCH = "07_ScrapingToolkit/policy_sources/batch_a.yaml"
DEFAULT_INCENTIVE_BATCH = "07_ScrapingToolkit/incentive_sources/batch_a.yaml"
DEFAULT_SPEC_BATCH = "07_ScrapingToolkit/spec_sources/batch_a.yaml"
DEFAULT_REQUIRED_KINDS = ("msrp", "news", "voc", "policy", "incentive", "spec")
DEFAULT_INTELLIGENCE_15_COUNTRIES = (
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return repo_root / path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalized_country_codes(countries: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for country in countries:
        code = str(country).strip().lower()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(normalized)


def _country_requirements_by_kind(
    required_countries: Sequence[str],
    required_kinds: Sequence[str],
    required_countries_by_kind: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    default_required = _normalized_country_codes(required_countries)
    overrides = {
        str(kind).strip().lower(): _normalized_country_codes(countries)
        for kind, countries in (required_countries_by_kind or {}).items()
        if str(kind).strip()
    }
    return {
        kind: overrides.get(kind, default_required)
        for kind in required_kinds
    }


def _load_yaml_mapping(path: Path) -> Mapping[str, Any] | None:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, Mapping) else None


def _iter_msrp_yaml_files(base: Path) -> Iterable[Path]:
    for path in sorted(base.rglob("*.yaml")):
        relative_parts = path.relative_to(base).parts
        if path.name.startswith("_") or any(part.startswith("_") for part in relative_parts):
            continue
        yield path


def _job_country(job: ScrapeJob) -> str:
    value = job.metadata.get("countryCode")
    return str(value or "").strip().lower()


def _mapping_error(
    *,
    kind: str,
    path: Path,
    base: Path,
    exc: Exception,
) -> dict[str, str]:
    try:
        rendered_path = str(path.relative_to(base))
    except ValueError:
        rendered_path = str(path)
    return {
        "kind": kind,
        "path": rendered_path,
        "errorClass": type(exc).__name__,
        "error": str(exc),
    }


def load_msrp_jobs_for_audit(
    msrp_dir: str | Path,
) -> tuple[list[ScrapeJob], list[dict[str, str]], int]:
    """Load MSRP source YAMLs into ScrapeJob without failing the full audit."""
    base = Path(msrp_dir).expanduser().resolve()
    jobs: list[ScrapeJob] = []
    errors: list[dict[str, str]] = []
    skipped = 0
    required_keys = {"source_code", "source_url", "extractor_type", "profile"}

    for path in _iter_msrp_yaml_files(base):
        try:
            data = _load_yaml_mapping(path)
            if data is None or not required_keys.issubset(data):
                skipped += 1
                continue
            jobs.append(msrp_source_to_scrape_job(data, source_path=path))
        except Exception as exc:  # noqa: BLE001 - diagnostic must keep scanning.
            errors.append(_mapping_error(kind="msrp", path=path, base=base, exc=exc))

    return jobs, errors, skipped


def load_news_jobs_for_audit(
    news_batch: str | Path,
) -> tuple[list[ScrapeJob], list[dict[str, str]]]:
    path = Path(news_batch).expanduser().resolve()
    jobs: list[ScrapeJob] = []
    errors: list[dict[str, str]] = []
    try:
        batch = load_news_batch_config(path)
    except Exception as exc:  # noqa: BLE001
        return [], [_mapping_error(kind="news", path=path, base=path.parent, exc=exc)]

    for country in batch.countries:
        for feed in country.feeds:
            try:
                jobs.append(news_feed_to_scrape_job(feed))
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {
                        "kind": "news",
                        "path": f"{path.name}:{feed.source_code}",
                        "errorClass": type(exc).__name__,
                        "error": str(exc),
                    }
                )
    return jobs, errors


def load_voc_jobs_for_audit(
    voc_batch: str | Path,
) -> tuple[list[ScrapeJob], list[dict[str, str]]]:
    path = Path(voc_batch).expanduser().resolve()
    jobs: list[ScrapeJob] = []
    errors: list[dict[str, str]] = []
    try:
        batch = load_voc_batch_config(path)
    except Exception as exc:  # noqa: BLE001
        return [], [_mapping_error(kind="voc", path=path, base=path.parent, exc=exc)]

    for country in batch.countries:
        for source in country.sources:
            try:
                jobs.append(voc_source_to_scrape_job(source))
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {
                        "kind": "voc",
                        "path": f"{path.name}:{source.source_code}",
                        "errorClass": type(exc).__name__,
                        "error": str(exc),
                    }
                )
    return jobs, errors


def load_domain_jobs_for_audit(
    batch_file: str | Path,
    *,
    kind: str,
) -> tuple[list[ScrapeJob], list[dict[str, str]]]:
    path = Path(batch_file).expanduser().resolve()
    try:
        jobs = load_domain_scrape_jobs_from_batch(path, kind=kind)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        return [], [_mapping_error(kind=kind, path=path, base=path.parent, exc=exc)]
    return jobs, []


def _country_coverage(
    jobs: Sequence[ScrapeJob],
    required_countries: Sequence[str],
    required_kinds: Sequence[str],
) -> dict[str, dict[str, int]]:
    countries = set(required_countries)
    countries.update(_job_country(job) for job in jobs if _job_country(job))
    coverage = {
        country: {kind: 0 for kind in required_kinds}
        for country in sorted(countries)
    }
    for job in jobs:
        country = _job_country(job)
        if not country:
            continue
        coverage.setdefault(country, {kind: 0 for kind in required_kinds})
        if job.kind in coverage[country]:
            coverage[country][job.kind] += 1
    return coverage


def _status_from_warnings(total_jobs: int, warnings: Sequence[str]) -> str:
    if total_jobs == 0:
        return "failed"
    if warnings:
        return "degraded"
    return "ok"


def run_audit(
    *,
    repo_root: str | Path | None = None,
    msrp_dir: str | Path = DEFAULT_MSRP_DIR,
    news_batch: str | Path = DEFAULT_NEWS_BATCH,
    voc_batch: str | Path = DEFAULT_VOC_BATCH,
    policy_batch: str | Path = DEFAULT_POLICY_BATCH,
    incentive_batch: str | Path = DEFAULT_INCENTIVE_BATCH,
    spec_batch: str | Path = DEFAULT_SPEC_BATCH,
    required_countries: Sequence[str] = DEFAULT_REQUIRED_COUNTRIES,
    required_kinds: Sequence[str] = DEFAULT_REQUIRED_KINDS,
    required_countries_by_kind: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    resolved_msrp_dir = _resolve_path(root, msrp_dir)
    resolved_news_batch = _resolve_path(root, news_batch)
    resolved_voc_batch = _resolve_path(root, voc_batch)
    resolved_policy_batch = _resolve_path(root, policy_batch)
    resolved_incentive_batch = _resolve_path(root, incentive_batch)
    resolved_spec_batch = _resolve_path(root, spec_batch)
    required = _normalized_country_codes(required_countries)
    required_job_kinds = tuple(
        kind.strip().lower()
        for kind in required_kinds
        if kind and kind.strip()
    )
    required_by_kind = _country_requirements_by_kind(
        required,
        required_job_kinds,
        required_countries_by_kind,
    )

    msrp_jobs, msrp_errors, skipped_msrp = load_msrp_jobs_for_audit(resolved_msrp_dir)
    news_jobs, news_errors = load_news_jobs_for_audit(resolved_news_batch)
    voc_jobs, voc_errors = load_voc_jobs_for_audit(resolved_voc_batch)
    policy_jobs, policy_errors = load_domain_jobs_for_audit(
        resolved_policy_batch,
        kind="policy",
    )
    incentive_jobs, incentive_errors = load_domain_jobs_for_audit(
        resolved_incentive_batch,
        kind="incentive",
    )
    spec_jobs, spec_errors = load_domain_jobs_for_audit(
        resolved_spec_batch,
        kind="spec",
    )
    jobs = [
        *msrp_jobs,
        *news_jobs,
        *voc_jobs,
        *policy_jobs,
        *incentive_jobs,
        *spec_jobs,
    ]
    mapping_errors = [
        *msrp_errors,
        *news_errors,
        *voc_errors,
        *policy_errors,
        *incentive_errors,
        *spec_errors,
    ]

    required_country_union = _normalized_country_codes(
        country
        for countries in required_by_kind.values()
        for country in countries
    )
    coverage = _country_coverage(
        jobs,
        required_country_union,
        required_job_kinds,
    )
    warnings: list[str] = []
    if mapping_errors:
        warnings.append(f"mapping_errors_present:{len(mapping_errors)}")
    for kind in required_job_kinds:
        for country in required_by_kind.get(kind, ()):
            country_counts = coverage.get(
                country,
                {job_kind: 0 for job_kind in required_job_kinds},
            )
            if country_counts.get(kind, 0) == 0:
                warnings.append(f"missing_{kind}_coverage:{country}")

    summary = {
        "totalJobs": len(jobs),
        "jobsByKind": dict(sorted(Counter(job.kind for job in jobs).items())),
        "jobsByFetcher": dict(sorted(Counter(job.fetcher for job in jobs).items())),
        "jobsByExtractor": dict(sorted(Counter(job.extractor for job in jobs).items())),
        "schemaRefs": dict(sorted(Counter(job.schema_ref for job in jobs).items())),
        "countryCoverage": coverage,
        "mappingErrorCount": len(mapping_errors),
        "skippedMsrpConfigCount": skipped_msrp,
    }

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": _status_from_warnings(len(jobs), warnings),
        "generatedAtUtc": _utc_now_iso(),
        "inputs": {
            "msrpDir": str(resolved_msrp_dir),
            "newsBatch": str(resolved_news_batch),
            "vocBatch": str(resolved_voc_batch),
            "policyBatch": str(resolved_policy_batch),
            "incentiveBatch": str(resolved_incentive_batch),
            "specBatch": str(resolved_spec_batch),
            "requiredCountries": list(required),
            "requiredKinds": list(required_job_kinds),
            "requiredCountriesByKind": {
                kind: list(countries)
                for kind, countries in required_by_kind.items()
            },
        },
        "summary": summary,
        "mappingErrors": mapping_errors,
        "warnings": warnings,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit local MSRP/News/VOC configs against ScrapeJob contracts.",
    )
    parser.add_argument("--repo-root", default=str(_repo_root()))
    parser.add_argument("--msrp-dir", default=DEFAULT_MSRP_DIR)
    parser.add_argument("--news-batch", default=DEFAULT_NEWS_BATCH)
    parser.add_argument("--voc-batch", default=DEFAULT_VOC_BATCH)
    parser.add_argument("--policy-batch", default=DEFAULT_POLICY_BATCH)
    parser.add_argument("--incentive-batch", default=DEFAULT_INCENTIVE_BATCH)
    parser.add_argument("--spec-batch", default=DEFAULT_SPEC_BATCH)
    parser.add_argument(
        "--required-countries",
        default=",".join(DEFAULT_REQUIRED_COUNTRIES),
        help="Comma-separated country codes that must have required jobs.",
    )
    parser.add_argument(
        "--required-kinds",
        default=",".join(DEFAULT_REQUIRED_KINDS),
        help="Comma-separated scrape kinds required for each country.",
    )
    parser.add_argument(
        "--required-countries-for-kind",
        action="append",
        default=[],
        metavar="KIND=CC,CC",
        help=(
            "Override required countries for one kind. Repeatable; useful for "
            "checking News/VOC 15-country coverage while policy/spec remain Nordic."
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the audit is degraded.",
    )
    return parser.parse_args(argv)


def _parse_required_countries_for_kind(
    values: Sequence[str] | None,
) -> dict[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for raw in values or []:
        if "=" not in raw:
            raise ValueError(
                "--required-countries-for-kind must be formatted as kind=cc,cc"
            )
        kind, countries = raw.split("=", 1)
        normalized_kind = kind.strip().lower()
        if not normalized_kind:
            raise ValueError("--required-countries-for-kind requires a kind")
        country_codes = _normalized_country_codes(
            part.strip()
            for part in countries.split(",")
            if part.strip()
        )
        if not country_codes:
            raise ValueError(
                f"--required-countries-for-kind {normalized_kind}=... has no countries"
            )
        result[normalized_kind] = country_codes
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    required_countries = [
        part.strip()
        for part in str(args.required_countries).split(",")
        if part.strip()
    ]
    required_kinds = [
        part.strip()
        for part in str(args.required_kinds).split(",")
        if part.strip()
    ]
    report = run_audit(
        repo_root=args.repo_root,
        msrp_dir=args.msrp_dir,
        news_batch=args.news_batch,
        voc_batch=args.voc_batch,
        policy_batch=args.policy_batch,
        incentive_batch=args.incentive_batch,
        spec_batch=args.spec_batch,
        required_countries=required_countries,
        required_kinds=required_kinds,
        required_countries_by_kind=_parse_required_countries_for_kind(
            args.required_countries_for_kind,
        ),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] == "failed":
        return 1
    if args.strict and report["status"] != "ok":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
