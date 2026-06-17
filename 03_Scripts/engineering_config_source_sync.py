#!/usr/bin/env python3
"""Official configuration/spec source sync readiness report.

This is intentionally network-free. It proves that official spec/configuration
sources are mapped through the unified ScrapeJob contract and have a declared
Engineering Config warehouse landing path before live fetchers are enabled.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
for path in (TOOLKIT_ROOT, HERMES_SCRIPT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from jato_scraper.core import (  # noqa: E402
    ScrapeJob,
    load_domain_scrape_jobs_from_batch,
    run_fixture_pipeline_for_job,
)

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - optional for unit tests.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "engineering_config_source_sync_v1"
PIPELINE_ID = "engineering_config_source_sync"
DEFAULT_SPEC_BATCH = "07_ScrapingToolkit/spec_sources/batch_a.yaml"
DEFAULT_REQUIRED_COUNTRIES = ("se", "fi", "no", "dk")
WAREHOUSE_CONTRACT = {
    "domain": "engineering_config",
    "importBatchDomain": "engineering_config",
    "schemaRef": "SpecFeatureObservation",
    "tables": [
        "ops.import_batches",
        "engineering_config.vehicle_trims",
        "engineering_config.trim_feature_values",
        "engineering_config.config_versions",
    ],
    "apiRoutes": [
        "POST /engineering-config/matrix/upload/{upload_id}/parse",
        "POST /engineering-config/matrix/upload/{upload_id}/confirm",
        "POST /engineering-config/matrix/upload/{upload_id}/import",
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else repo_root / path


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
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
    stamp = (
        generated.replace(":", "")
        .replace("-", "")
        .replace("+", "z")
        .replace(".", "-")
    )
    return _safe_token(stamp, "unknown-time")


def _csv_arg(value: str | Sequence[str]) -> tuple[str, ...]:
    parts = value.split(",") if isinstance(value, str) else value
    normalized: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = str(part).strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return tuple(normalized)


def _job_country(job: ScrapeJob) -> str:
    return str(job.metadata.get("countryCode") or "").strip().lower()


def _job_brand(job: ScrapeJob) -> str:
    return str(job.metadata.get("brand") or "").strip().upper()


def _job_model(job: ScrapeJob) -> str:
    return str(job.metadata.get("model") or job.metadata.get("official_model") or "").strip()


def _summarize_job(job: ScrapeJob) -> dict[str, Any]:
    metadata = job.metadata
    return {
        "jobId": job.job_id,
        "country": _job_country(job),
        "brand": _job_brand(job),
        "model": _job_model(job),
        "sourceCode": metadata.get("sourceCode"),
        "sourceName": metadata.get("sourceName"),
        "sourceKind": metadata.get("sourceKind"),
        "topics": metadata.get("topics") or [],
        "url": job.url,
        "fetcher": job.fetcher,
        "extractor": job.extractor,
        "schemaRef": job.schema_ref,
        "freshnessHours": job.freshness.max_age_hours,
        "priority": job.priority,
        "allowDomains": job.allow_domains,
    }


def _select_stage_jobs(
    jobs: Sequence[ScrapeJob],
    *,
    sample_per_country: int,
) -> list[ScrapeJob]:
    by_country: dict[str, list[ScrapeJob]] = defaultdict(list)
    for job in sorted(jobs, key=lambda item: item.job_id):
        by_country[_job_country(job)].append(job)

    selected: list[ScrapeJob] = []
    for country in sorted(by_country):
        selected.extend(by_country[country][: max(1, int(sample_per_country))])
    return selected


def _run_stage_smoke(
    jobs: Sequence[ScrapeJob],
    *,
    artifact_root: Path,
    sample_per_country: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for job in _select_stage_jobs(jobs, sample_per_country=sample_per_country):
        try:
            output = run_fixture_pipeline_for_job(job, artifact_root=artifact_root)
        except Exception as exc:  # noqa: BLE001 - readiness must keep scanning.
            errors.append(
                {
                    "jobId": job.job_id,
                    "errorClass": type(exc).__name__,
                    "error": str(exc),
                }
            )
            continue
        sink = output["sinkResult"]
        normalized = output["normalizedObservations"][0]
        results.append(
            {
                "jobId": job.job_id,
                "country": _job_country(job),
                "schemaRef": job.schema_ref,
                "recordKey": normalized.record_key,
                "quality": normalized.quality,
                "sink": sink.model_dump(mode="json"),
            }
        )
    return results, errors


def _build_summary(
    jobs: Sequence[ScrapeJob],
    *,
    required_countries: Sequence[str],
) -> dict[str, Any]:
    countries = sorted({country for job in jobs if (country := _job_country(job))})
    missing = [
        country for country in required_countries
        if country not in set(countries)
    ]
    return {
        "sourceCount": len(jobs),
        "countryCount": len(countries),
        "countries": countries,
        "missingRequiredCountries": missing,
        "brands": sorted({brand for job in jobs if (brand := _job_brand(job))}),
        "models": sorted({model for job in jobs if (model := _job_model(job))}),
        "jobsByFetcher": dict(sorted(Counter(job.fetcher for job in jobs).items())),
        "jobsByExtractor": dict(sorted(Counter(job.extractor for job in jobs).items())),
        "schemaRefs": dict(sorted(Counter(job.schema_ref for job in jobs).items())),
        "sourceKinds": dict(
            sorted(
                Counter(
                    str(job.metadata.get("sourceKind") or "unknown")
                    for job in jobs
                ).items()
            )
        ),
    }


def build_source_sync_report(
    *,
    repo_root: str | Path | None = None,
    spec_batch: str | Path = DEFAULT_SPEC_BATCH,
    required_countries: Sequence[str] = DEFAULT_REQUIRED_COUNTRIES,
    artifact_root: str | Path | None = None,
    sample_per_country: int = 1,
    run_stage_smoke: bool = True,
) -> dict[str, Any]:
    started = time.time()
    root = Path(repo_root).expanduser().resolve() if repo_root else REPO_ROOT
    resolved_spec_batch = _resolve_path(root, spec_batch)
    normalized_required = _csv_arg(required_countries)
    mapping_errors: list[dict[str, str]] = []
    stage_results: list[dict[str, Any]] = []
    stage_errors: list[dict[str, str]] = []
    stage_artifact_root: str | None = None

    try:
        jobs = load_domain_scrape_jobs_from_batch(resolved_spec_batch, kind="spec")
    except Exception as exc:  # noqa: BLE001 - report the exact mapping blocker.
        jobs = []
        mapping_errors.append(
            {
                "path": str(resolved_spec_batch),
                "errorClass": type(exc).__name__,
                "error": str(exc),
            }
        )

    if run_stage_smoke and jobs:
        stage_root = (
            _resolve_path(root, artifact_root)
            if artifact_root is not None
            else Path(tempfile.mkdtemp(prefix="jato_config_source_sync_"))
        )
        stage_artifact_root = str(stage_root)
        stage_results, stage_errors = _run_stage_smoke(
            jobs,
            artifact_root=stage_root,
            sample_per_country=max(1, int(sample_per_country)),
        )

    summary = _build_summary(jobs, required_countries=normalized_required)
    warnings: list[str] = []
    for country in summary["missingRequiredCountries"]:
        warnings.append(f"missing_spec_source_country:{country}")
    if not jobs:
        warnings.append("no_spec_sources_loaded")
    if mapping_errors:
        warnings.append(f"mapping_errors_present:{len(mapping_errors)}")
    if stage_errors:
        warnings.append(f"stage_errors_present:{len(stage_errors)}")

    status = "passed"
    if not jobs or mapping_errors or stage_errors:
        status = "failed"
    elif warnings:
        status = "degraded"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": status,
        "generatedAtUtc": _utc_now_iso(),
        "elapsedSeconds": round(time.time() - started, 2),
        "inputs": {
            "specBatch": str(resolved_spec_batch),
            "requiredCountries": list(normalized_required),
            "runStageSmoke": run_stage_smoke,
            "samplePerCountry": max(1, int(sample_per_country)),
        },
        "summary": {
            **summary,
            "stageSampledCount": len(stage_results),
            "stageErrorCount": len(stage_errors),
            "mappingErrorCount": len(mapping_errors),
            "warningCount": len(warnings),
        },
        "warehouseContract": WAREHOUSE_CONTRACT,
        "sources": [_summarize_job(job) for job in jobs],
        "stageSmoke": {
            "status": "not_run"
            if not run_stage_smoke
            else "ok"
            if stage_results and not stage_errors
            else "failed",
            "artifactRoot": stage_artifact_root,
            "results": stage_results,
            "errors": stage_errors,
        },
        "mappingErrors": mapping_errors,
        "warnings": warnings,
    }


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    warehouse = (
        report.get("warehouseContract")
        if isinstance(report.get("warehouseContract"), dict)
        else {}
    )
    lines = [
        "# Engineering Config Source Sync",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Sources | {summary.get('sourceCount', 0)} |",
        f"| Countries | {summary.get('countryCount', 0)} |",
        f"| Stage sampled | {summary.get('stageSampledCount', 0)} |",
        f"| Mapping errors | {summary.get('mappingErrorCount', 0)} |",
        f"| Warnings | {summary.get('warningCount', 0)} |",
        "",
        "## Warehouse Contract",
        "",
        f"- Domain: {_markdown_cell(warehouse.get('domain'))}",
        f"- Schema ref: {_markdown_cell(warehouse.get('schemaRef'))}",
        f"- Tables: {_markdown_cell(', '.join(warehouse.get('tables') or []))}",
        "",
        "## Sources",
        "",
        "| Country | Brand | Model | Source | Schema |",
        "|---|---|---|---|---|",
    ]
    for source in report.get("sources") or []:
        if not isinstance(source, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(source.get("country")),
                    _markdown_cell(source.get("brand")),
                    _markdown_cell(source.get("model")),
                    _markdown_cell(source.get("sourceCode")),
                    _markdown_cell(source.get("schemaRef")),
                ]
            )
            + " |"
        )
    warnings = [str(item) for item in report.get("warnings") or [] if str(item).strip()]
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {_markdown_cell(warning)}" for warning in warnings)
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "engineering_config_source_sync.json"
    latest_md = output_root / "engineering_config_source_sync.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"engineering_config_source_sync_{suffix}.json"
    hist_md = output_root / f"engineering_config_source_sync_{suffix}.md"
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
    started_at: str,
    artifact_refs: Sequence[str],
) -> dict[str, Any] | None:
    if write_pipeline_status is None:
        return None
    status = str(report.get("status") or "failed")
    pipeline_status = (
        "success" if status == "passed" else "degraded" if status == "degraded" else "failed"
    )
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return write_pipeline_status(
        pipeline_id=PIPELINE_ID,
        status=pipeline_status,
        started_at=started_at,
        finished_at=_utc_now_iso(),
        exit_code=0 if status in {"passed", "degraded"} else 1,
        records_processed=int(summary.get("sourceCount") or 0),
        failed_count=int(summary.get("mappingErrorCount") or 0)
        + int(summary.get("stageErrorCount") or 0),
        warning_count=int(summary.get("warningCount") or 0),
        artifact_refs=list(artifact_refs),
        source=PIPELINE_ID,
        message=(
            f"Engineering config source sync={status}; "
            f"sources={summary.get('sourceCount', 0)}, "
            f"countries={summary.get('countryCount', 0)}, "
            f"warnings={summary.get('warningCount', 0)}."
        ),
        extra={
            "sourceSyncStatus": status,
            "sourceCount": summary.get("sourceCount", 0),
            "countryCount": summary.get("countryCount", 0),
            "countries": summary.get("countries") or [],
            "schemaRefs": summary.get("schemaRefs") or {},
            "warehouseContract": report.get("warehouseContract") or {},
        },
        repo_root=REPO_ROOT,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an official configuration/spec source sync readiness report.",
    )
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--spec-batch", default=DEFAULT_SPEC_BATCH)
    parser.add_argument(
        "--required-countries",
        default=",".join(DEFAULT_REQUIRED_COUNTRIES),
    )
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--sample-per-country", type=int, default=1)
    parser.add_argument("--no-stage-smoke", action="store_true")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--write-status", action="store_true")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = _utc_now_iso()
    report = build_source_sync_report(
        repo_root=args.repo_root,
        spec_batch=args.spec_batch,
        required_countries=_csv_arg(args.required_countries),
        artifact_root=args.artifact_root,
        sample_per_country=max(1, int(args.sample_per_country)),
        run_stage_smoke=not bool(args.no_stage_smoke),
    )
    artifact_refs: dict[str, str] = {}
    if args.out_dir:
        artifact_refs = write_outputs(report, args.out_dir)
        report["artifacts"] = artifact_refs
    if args.write_status:
        status_record = write_status_record(
            report,
            started_at=started_at,
            artifact_refs=list(artifact_refs.values()),
        )
        if status_record is not None:
            report["pipelineStatus"] = status_record
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] == "failed":
        return 1
    if args.strict and report["status"] != "passed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
