#!/usr/bin/env python3
"""Readiness audit for the unified scraping pipeline.

This combines the static ScrapeJob contract audit, the local stage smoke, and
the News/VOC intelligence smoke so deploy checks can rely on one Hermes
artifact/status record.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import time
from typing import Any, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
for path in (SCRIPT_DIR, TOOLKIT_ROOT, HERMES_SCRIPT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from unified_scraping_contract_audit import (  # noqa: E402
    DEFAULT_INCENTIVE_BATCH,
    DEFAULT_MSRP_DIR,
    DEFAULT_NEWS_BATCH,
    DEFAULT_POLICY_BATCH,
    DEFAULT_REQUIRED_COUNTRIES,
    DEFAULT_REQUIRED_KINDS,
    DEFAULT_SPEC_BATCH,
    DEFAULT_VOC_BATCH,
    _parse_required_countries_for_kind,
    run_audit,
)
from ai_intelligence_enrichment_smoke import (  # noqa: E402
    DEFAULT_REQUIRED_COUNTRIES as DEFAULT_INTELLIGENCE_COUNTRIES,
    run_smoke as run_intelligence_smoke,
)
from unified_scraping_stage_smoke import run_stage_smoke  # noqa: E402

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - optional when imported in isolation.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "unified_scraping_readiness_v1"
PIPELINE_ID = "unified_scraping_readiness"


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
    stamp = (
        generated.replace(":", "")
        .replace("-", "")
        .replace("+", "z")
        .replace(".", "-")
    )
    return _safe_token(stamp, "unknown-time")


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _csv_arg(value: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = value
    return tuple(
        str(part).strip().lower()
        for part in parts
        if str(part).strip()
    )


def _combined_status(status_values: Sequence[str]) -> str:
    statuses = {status for status in status_values if status != "skipped"}
    if "failed" in statuses:
        return "failed"
    if not statuses.issubset({"ok", "degraded"}):
        return "failed"
    if "degraded" in statuses:
        return "degraded"
    return "passed"


def _prefixed_warnings(prefix: str, report: dict[str, Any]) -> list[str]:
    return [
        f"{prefix}:{warning}"
        for warning in report.get("warnings") or []
        if str(warning).strip()
    ]


def _child_artifact_root(
    artifact_root: str | Path | None,
    child_name: str,
) -> str | None:
    if artifact_root is None:
        return None
    return str(Path(artifact_root).expanduser() / child_name)


def _intelligence_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    if not report:
        return {
            "status": "skipped",
            "requiredCountryCount": 0,
            "newsCountryCount": 0,
            "newsMarketEventCount": 0,
            "newsMissingEvidenceCountryCount": 0,
            "vocCountryCount": 0,
            "vocDocumentCount": 0,
            "vocSignalObservationCount": 0,
            "vocMissingEvidenceCountryCount": 0,
        }
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    news = summary.get("news") if isinstance(summary.get("news"), dict) else {}
    voc = summary.get("voc") if isinstance(summary.get("voc"), dict) else {}
    return {
        "status": str(report.get("status") or "failed"),
        "requiredCountryCount": int(summary.get("requiredCountryCount") or 0),
        "newsCountryCount": int(news.get("countryCount") or 0),
        "newsMarketEventCount": int(news.get("marketEventCount") or 0),
        "newsMissingEvidenceCountryCount": len(
            news.get("missingEvidenceCountries") or []
        ),
        "vocCountryCount": int(voc.get("countryCount") or 0),
        "vocDocumentCount": int(voc.get("documentCount") or 0),
        "vocSignalObservationCount": int(voc.get("signalObservationCount") or 0),
        "vocMissingEvidenceCountryCount": len(
            voc.get("missingEvidenceCountries") or []
        ),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    jobs_by_kind = (
        summary.get("jobsByKind")
        if isinstance(summary.get("jobsByKind"), dict)
        else {}
    )
    warnings = [str(item) for item in report.get("warnings") or [] if str(item).strip()]
    contract = report.get("contract") if isinstance(report.get("contract"), dict) else {}
    stage = report.get("stageSmoke") if isinstance(report.get("stageSmoke"), dict) else {}
    stage_summary = (
        stage.get("summary")
        if isinstance(stage.get("summary"), dict)
        else {}
    )
    intelligence = (
        report.get("intelligenceSmoke")
        if isinstance(report.get("intelligenceSmoke"), dict)
        else {}
    )
    intelligence_summary = (
        summary.get("intelligence")
        if isinstance(summary.get("intelligence"), dict)
        else {}
    )

    lines: list[str] = [
        "# Unified Scraping Readiness",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        f"**Contract:** {summary.get('contractStatus', '-')}",
        f"**Stage smoke:** {summary.get('stageStatus', '-')}",
        f"**AI intelligence:** {summary.get('intelligenceStatus', '-')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Configured jobs | {summary.get('configuredJobCount', 0)} |",
        f"| Sampled jobs | {summary.get('sampledJobCount', 0)} |",
        f"| Failed stages | {summary.get('failedStageCount', 0)} |",
        f"| Mapping errors | {summary.get('mappingErrorCount', 0)} |",
        f"| Intelligence countries | {intelligence_summary.get('requiredCountryCount', 0)} |",
        f"| News market events | {intelligence_summary.get('newsMarketEventCount', 0)} |",
        f"| VOC documents | {intelligence_summary.get('vocDocumentCount', 0)} |",
        f"| VOC signal observations | {intelligence_summary.get('vocSignalObservationCount', 0)} |",
        f"| Warnings | {summary.get('warningCount', 0)} |",
        "",
        "## Jobs By Kind",
        "",
        "| Kind | Jobs | Sampled |",
        "|---|---:|---:|",
    ]
    sampled_by_kind = (
        stage_summary.get("sampledByKind")
        if isinstance(stage_summary.get("sampledByKind"), dict)
        else {}
    )
    for kind in sorted(set(jobs_by_kind) | set(sampled_by_kind)):
        lines.append(
            f"| {_markdown_cell(kind)} | "
            f"{jobs_by_kind.get(kind, 0)} | "
            f"{sampled_by_kind.get(kind, 0)} |"
        )

    contract_inputs = (
        contract.get("inputs")
        if isinstance(contract.get("inputs"), dict)
        else {}
    )
    if contract_inputs:
        lines.extend(
            [
                "",
                "## Required Coverage",
                "",
                f"- Required kinds: {_markdown_cell(', '.join(contract_inputs.get('requiredKinds') or []))}",
                f"- Default countries: {_markdown_cell(', '.join(contract_inputs.get('requiredCountries') or []))}",
            ]
        )
    intelligence_inputs = (
        intelligence.get("inputs")
        if isinstance(intelligence.get("inputs"), dict)
        else {}
    )
    if intelligence_inputs:
        lines.extend(
            [
                "",
                "## AI Intelligence",
                "",
                f"- Required countries: {_markdown_cell(', '.join(intelligence_inputs.get('requiredCountries') or []))}",
                f"- News countries: {intelligence_summary.get('newsCountryCount', 0)}",
                f"- VOC countries: {intelligence_summary.get('vocCountryCount', 0)}",
                f"- Missing news evidence countries: {intelligence_summary.get('newsMissingEvidenceCountryCount', 0)}",
                f"- Missing VOC evidence countries: {intelligence_summary.get('vocMissingEvidenceCountryCount', 0)}",
            ]
        )
    artifact_root = stage.get("artifactRoot")
    if artifact_root:
        lines.extend(["", "## Stage Artifacts", "", f"- {_markdown_cell(artifact_root)}"])
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {_markdown_cell(warning)}" for warning in warnings)
    return "\n".join(lines) + "\n"


def build_readiness_report(
    *,
    repo_root: str | Path | None = None,
    artifact_root: str | Path | None = None,
    msrp_dir: str | Path = DEFAULT_MSRP_DIR,
    news_batch: str | Path = DEFAULT_NEWS_BATCH,
    voc_batch: str | Path = DEFAULT_VOC_BATCH,
    policy_batch: str | Path = DEFAULT_POLICY_BATCH,
    incentive_batch: str | Path = DEFAULT_INCENTIVE_BATCH,
    spec_batch: str | Path = DEFAULT_SPEC_BATCH,
    required_countries: Sequence[str] = DEFAULT_REQUIRED_COUNTRIES,
    required_kinds: Sequence[str] = DEFAULT_REQUIRED_KINDS,
    required_countries_by_kind: dict[str, Sequence[str]] | None = None,
    sample_per_kind: int = 1,
    include_intelligence_smoke: bool = True,
    intelligence_countries: Sequence[str] | None = None,
) -> dict[str, Any]:
    started = time.time()
    contract_report = run_audit(
        repo_root=repo_root,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_countries=required_countries,
        required_kinds=required_kinds,
        required_countries_by_kind=required_countries_by_kind,
    )
    stage_report = run_stage_smoke(
        repo_root=repo_root,
        artifact_root=artifact_root,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_kinds=required_kinds,
        sample_per_kind=sample_per_kind,
    )
    intelligence_report = (
        run_intelligence_smoke(
            repo_root=repo_root,
            news_batch=news_batch,
            voc_batch=voc_batch,
            artifact_root=_child_artifact_root(artifact_root, "intelligence"),
            required_countries=(
                intelligence_countries
                if intelligence_countries is not None
                else DEFAULT_INTELLIGENCE_COUNTRIES
            ),
        )
        if include_intelligence_smoke
        else None
    )
    contract_status = str(contract_report.get("status") or "failed")
    stage_status = str(stage_report.get("status") or "failed")
    intelligence_status = (
        str(intelligence_report.get("status") or "failed")
        if intelligence_report is not None
        else "skipped"
    )
    warnings = [
        *_prefixed_warnings("contract", contract_report),
        *_prefixed_warnings("stage", stage_report),
        *(
            _prefixed_warnings("intelligence", intelligence_report)
            if intelligence_report is not None
            else []
        ),
    ]
    contract_summary = (
        contract_report.get("summary")
        if isinstance(contract_report.get("summary"), dict)
        else {}
    )
    stage_summary = (
        stage_report.get("summary")
        if isinstance(stage_report.get("summary"), dict)
        else {}
    )
    mapping_error_count = int(contract_summary.get("mappingErrorCount") or 0) + int(
        stage_summary.get("mappingErrorCount") or 0
    )
    failed_stage_count = int(stage_summary.get("failedStageCount") or 0)
    intelligence_summary = _intelligence_summary(intelligence_report)

    report: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "status": _combined_status(
            (contract_status, stage_status, intelligence_status)
        ),
        "generatedAtUtc": _utc_now_iso(),
        "elapsedSeconds": round(time.time() - started, 2),
        "summary": {
            "contractStatus": contract_status,
            "stageStatus": stage_status,
            "intelligenceStatus": intelligence_status,
            "configuredJobCount": int(contract_summary.get("totalJobs") or 0),
            "sampledJobCount": int(stage_summary.get("sampledJobCount") or 0),
            "jobsByKind": contract_summary.get("jobsByKind") or {},
            "failedStageCount": failed_stage_count,
            "mappingErrorCount": mapping_error_count,
            "intelligence": intelligence_summary,
            "warningCount": len(warnings),
        },
        "contract": contract_report,
        "stageSmoke": stage_report,
        "warnings": warnings,
    }
    if intelligence_report is not None:
        report["intelligenceSmoke"] = intelligence_report
    return report


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "unified_scraping_readiness.json"
    latest_md = output_root / "unified_scraping_readiness.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"unified_scraping_readiness_{suffix}.json"
    hist_md = output_root / f"unified_scraping_readiness_{suffix}.md"
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
        "success"
        if status == "passed"
        else "degraded"
        if status == "degraded"
        else "failed"
    )
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    warning_count = int(summary.get("warningCount") or 0)
    failed_count = 1 if status == "failed" else 0
    return write_pipeline_status(
        pipeline_id=PIPELINE_ID,
        status=pipeline_status,
        started_at=started_at,
        finished_at=_utc_now_iso(),
        exit_code=0 if status in {"passed", "degraded"} else 1,
        records_processed=int(summary.get("configuredJobCount") or 0),
        failed_count=failed_count,
        warning_count=warning_count,
        artifact_refs=list(artifact_refs),
        source=PIPELINE_ID,
        message=(
            f"Unified scraping readiness={status}; "
            f"contract={summary.get('contractStatus', '-')}, "
            f"stage={summary.get('stageStatus', '-')}, "
            f"intelligence={summary.get('intelligenceStatus', '-')}, "
            f"jobs={summary.get('configuredJobCount', 0)}, "
            f"sampled={summary.get('sampledJobCount', 0)}, "
            f"warnings={warning_count}."
        ),
        extra={
            "readinessStatus": status,
            "contractStatus": summary.get("contractStatus"),
            "stageStatus": summary.get("stageStatus"),
            "intelligenceStatus": summary.get("intelligenceStatus"),
            "jobsByKind": summary.get("jobsByKind") or {},
            "failedStageCount": summary.get("failedStageCount", 0),
            "mappingErrorCount": summary.get("mappingErrorCount", 0),
            "intelligence": summary.get("intelligence") or {},
        },
        repo_root=REPO_ROOT,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit unified scraping contract and local stage readiness.",
    )
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--msrp-dir", default=DEFAULT_MSRP_DIR)
    parser.add_argument("--news-batch", default=DEFAULT_NEWS_BATCH)
    parser.add_argument("--voc-batch", default=DEFAULT_VOC_BATCH)
    parser.add_argument("--policy-batch", default=DEFAULT_POLICY_BATCH)
    parser.add_argument("--incentive-batch", default=DEFAULT_INCENTIVE_BATCH)
    parser.add_argument("--spec-batch", default=DEFAULT_SPEC_BATCH)
    parser.add_argument(
        "--required-countries",
        default=",".join(DEFAULT_REQUIRED_COUNTRIES),
        help="Comma-separated country codes required for each default kind.",
    )
    parser.add_argument(
        "--required-kinds",
        default=",".join(DEFAULT_REQUIRED_KINDS),
        help="Comma-separated scrape kinds to verify.",
    )
    parser.add_argument(
        "--required-countries-for-kind",
        action="append",
        default=[],
        metavar="KIND=CC,CC",
        help=(
            "Override required countries for one kind. Repeatable; "
            "for example news=se,fi,no,dk,at,cz,hu,hr,de,fr,it,pl,sk,si,ch."
        ),
    )
    parser.add_argument("--sample-per-kind", type=int, default=1)
    parser.add_argument(
        "--intelligence-countries",
        default=",".join(DEFAULT_INTELLIGENCE_COUNTRIES),
        help="Comma-separated country codes to verify in News/VOC intelligence smoke.",
    )
    parser.add_argument(
        "--skip-intelligence-smoke",
        action="store_true",
        help="Skip News/VOC AI enrichment readiness checks.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional Hermes reports directory for JSON/Markdown artifacts.",
    )
    parser.add_argument(
        "--write-status",
        action="store_true",
        help="Write hermes/reports/pipeline_status/unified_scraping_readiness.json.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless readiness status is passed.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = _utc_now_iso()
    report = build_readiness_report(
        repo_root=args.repo_root,
        artifact_root=args.artifact_root,
        msrp_dir=args.msrp_dir,
        news_batch=args.news_batch,
        voc_batch=args.voc_batch,
        policy_batch=args.policy_batch,
        incentive_batch=args.incentive_batch,
        spec_batch=args.spec_batch,
        required_countries=_csv_arg(args.required_countries),
        required_kinds=_csv_arg(args.required_kinds),
        required_countries_by_kind=_parse_required_countries_for_kind(
            args.required_countries_for_kind,
        ),
        sample_per_kind=max(1, int(args.sample_per_kind)),
        include_intelligence_smoke=not bool(args.skip_intelligence_smoke),
        intelligence_countries=_csv_arg(args.intelligence_countries),
    )
    artifact_refs: dict[str, str] = {}
    if args.out_dir:
        artifact_refs = write_outputs(report, args.out_dir)
        report["reportArtifacts"] = artifact_refs
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
