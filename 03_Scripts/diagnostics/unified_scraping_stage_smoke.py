#!/usr/bin/env python3
"""Run a local stage smoke for unified scraping jobs.

The smoke is network-free. It samples configured ScrapeJob objects and runs
them through RawDocument -> StructuredObservation -> NormalizedObservation ->
JSONL SinkResult so the shared contracts are verifiable before real fetchers
or PG sinks are available for every kind.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from typing import Any, Sequence

from jato_scraper.core import ScrapeJob, run_fixture_pipeline_for_job

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from unified_scraping_contract_audit import (
    DEFAULT_INCENTIVE_BATCH,
    DEFAULT_MSRP_DIR,
    DEFAULT_NEWS_BATCH,
    DEFAULT_POLICY_BATCH,
    DEFAULT_REQUIRED_KINDS,
    DEFAULT_SPEC_BATCH,
    DEFAULT_VOC_BATCH,
    load_domain_jobs_for_audit,
    load_msrp_jobs_for_audit,
    load_news_jobs_for_audit,
    load_voc_jobs_for_audit,
)


SCHEMA_VERSION = "unified_scraping_stage_smoke_v1"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return repo_root / path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_jobs(
    *,
    repo_root: Path,
    msrp_dir: str | Path,
    news_batch: str | Path,
    voc_batch: str | Path,
    policy_batch: str | Path,
    incentive_batch: str | Path,
    spec_batch: str | Path,
) -> tuple[list[ScrapeJob], list[dict[str, str]]]:
    msrp_jobs, msrp_errors, _ = load_msrp_jobs_for_audit(
        _resolve_path(repo_root, msrp_dir)
    )
    news_jobs, news_errors = load_news_jobs_for_audit(
        _resolve_path(repo_root, news_batch)
    )
    voc_jobs, voc_errors = load_voc_jobs_for_audit(_resolve_path(repo_root, voc_batch))
    policy_jobs, policy_errors = load_domain_jobs_for_audit(
        _resolve_path(repo_root, policy_batch),
        kind="policy",
    )
    incentive_jobs, incentive_errors = load_domain_jobs_for_audit(
        _resolve_path(repo_root, incentive_batch),
        kind="incentive",
    )
    spec_jobs, spec_errors = load_domain_jobs_for_audit(
        _resolve_path(repo_root, spec_batch),
        kind="spec",
    )
    return (
        [
            *msrp_jobs,
            *news_jobs,
            *voc_jobs,
            *policy_jobs,
            *incentive_jobs,
            *spec_jobs,
        ],
        [
            *msrp_errors,
            *news_errors,
            *voc_errors,
            *policy_errors,
            *incentive_errors,
            *spec_errors,
        ],
    )


def _sample_jobs_by_kind(
    jobs: Sequence[ScrapeJob],
    required_kinds: Sequence[str],
    sample_per_kind: int,
) -> tuple[list[ScrapeJob], list[str]]:
    by_kind: dict[str, list[ScrapeJob]] = defaultdict(list)
    for job in sorted(jobs, key=lambda item: item.job_id):
        by_kind[job.kind].append(job)

    selected: list[ScrapeJob] = []
    warnings: list[str] = []
    for kind in required_kinds:
        kind_jobs = by_kind.get(kind, [])
        if not kind_jobs:
            warnings.append(f"missing_kind:{kind}")
            continue
        selected.extend(kind_jobs[: max(1, int(sample_per_kind))])
    return selected, warnings


def run_stage_smoke(
    *,
    repo_root: str | Path | None = None,
    artifact_root: str | Path | None = None,
    msrp_dir: str | Path = DEFAULT_MSRP_DIR,
    news_batch: str | Path = DEFAULT_NEWS_BATCH,
    voc_batch: str | Path = DEFAULT_VOC_BATCH,
    policy_batch: str | Path = DEFAULT_POLICY_BATCH,
    incentive_batch: str | Path = DEFAULT_INCENTIVE_BATCH,
    spec_batch: str | Path = DEFAULT_SPEC_BATCH,
    required_kinds: Sequence[str] = DEFAULT_REQUIRED_KINDS,
    sample_per_kind: int = 1,
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    output_root = (
        Path(artifact_root).expanduser().resolve()
        if artifact_root is not None
        else Path(tempfile.mkdtemp(prefix="jato_unified_stage_smoke_"))
    )
    normalized_kinds = tuple(
        str(kind).strip().lower()
        for kind in required_kinds
        if str(kind).strip()
    )
    jobs, mapping_errors = _load_jobs(
        repo_root=root,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
    )
    selected_jobs, warnings = _sample_jobs_by_kind(
        jobs,
        normalized_kinds,
        sample_per_kind,
    )
    if mapping_errors:
        warnings.append(f"mapping_errors_present:{len(mapping_errors)}")

    stage_results: list[dict[str, Any]] = []
    for job in selected_jobs:
        stage_output = run_fixture_pipeline_for_job(
            job,
            artifact_root=output_root,
        )
        sink = stage_output["sinkResult"]
        run_log = stage_output["runLog"]
        normalized = stage_output["normalizedObservations"][0]
        raw = stage_output["rawDocument"]
        stage_results.append(
            {
                "kind": job.kind,
                "jobId": job.job_id,
                "fetcher": job.fetcher,
                "extractor": job.extractor,
                "schemaRef": job.schema_ref,
                "rawDocument": {
                    "contentType": raw.content_type,
                    "statusCode": raw.metadata.status_code,
                    "finalUrl": raw.metadata.final_url,
                },
                "structuredCount": len(stage_output["structuredObservations"]),
                "normalizedCount": len(stage_output["normalizedObservations"]),
                "recordKey": normalized.record_key,
                "quality": normalized.quality,
                "sink": sink.model_dump(mode="json"),
                "runLog": run_log.model_dump(mode="json"),
            }
        )

    failed_stage_count = sum(
        1 for item in stage_results if item["sink"]["status"] != "ok"
    )
    status = "ok"
    if not stage_results or failed_stage_count:
        status = "failed"
    elif warnings:
        status = "degraded"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": status,
        "generatedAtUtc": _utc_now_iso(),
        "artifactRoot": str(output_root),
        "summary": {
            "configuredJobCount": len(jobs),
            "sampledJobCount": len(selected_jobs),
            "jobsByKind": dict(sorted(Counter(job.kind for job in jobs).items())),
            "sampledByKind": dict(
                sorted(Counter(item["kind"] for item in stage_results).items())
            ),
            "failedStageCount": failed_stage_count,
            "mappingErrorCount": len(mapping_errors),
        },
        "stageResults": stage_results,
        "mappingErrors": mapping_errors,
        "warnings": warnings,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local Fetcher->Extractor->Normalizer->Sink smoke for ScrapeJobs.",
    )
    parser.add_argument("--repo-root", default=str(_repo_root()))
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--msrp-dir", default=DEFAULT_MSRP_DIR)
    parser.add_argument("--news-batch", default=DEFAULT_NEWS_BATCH)
    parser.add_argument("--voc-batch", default=DEFAULT_VOC_BATCH)
    parser.add_argument("--policy-batch", default=DEFAULT_POLICY_BATCH)
    parser.add_argument("--incentive-batch", default=DEFAULT_INCENTIVE_BATCH)
    parser.add_argument("--spec-batch", default=DEFAULT_SPEC_BATCH)
    parser.add_argument("--required-kinds", default=",".join(DEFAULT_REQUIRED_KINDS))
    parser.add_argument("--sample-per-kind", type=int, default=1)
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    required_kinds = [
        part.strip()
        for part in str(args.required_kinds).split(",")
        if part.strip()
    ]
    report = run_stage_smoke(
        repo_root=args.repo_root,
        artifact_root=args.artifact_root,
        msrp_dir=args.msrp_dir,
        news_batch=args.news_batch,
        voc_batch=args.voc_batch,
        policy_batch=args.policy_batch,
        incentive_batch=args.incentive_batch,
        spec_batch=args.spec_batch,
        required_kinds=required_kinds,
        sample_per_kind=args.sample_per_kind,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] == "failed":
        return 1
    if args.strict and report["status"] != "ok":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
