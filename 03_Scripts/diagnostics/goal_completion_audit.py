#!/usr/bin/env python3
"""Completion audit for the MSRP/AI/Unified scraping goal.

This report is intentionally stricter than the P0 readiness checks. It keeps
local feature readiness separate from full PRD completion evidence such as
source-draft coverage and production deployment state.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import socket
import sys
from typing import Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[2]
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
if str(HERMES_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_SCRIPT_DIR))

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - optional for isolated unit imports.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "jato_goal_completion_audit_v1"
PIPELINE_ID = "goal_completion_audit"
DEFAULT_SOURCE_DRAFT_DIR = "07_ScrapingToolkit/source_drafts/suv_only_country_model_top30"
DEFAULT_REQUIRED_SOURCE_COUNTRIES = (
    "at",
    "be",
    "ch",
    "cz",
    "de",
    "dk",
    "es",
    "fi",
    "fr",
    "gr",
    "hr",
    "hu",
    "it",
    "nl",
    "no",
    "pl",
    "pt",
    "ro",
    "se",
    "si",
    "sk",
)
DEFAULT_REQUIRED_AI_COUNTRIES = (
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
REQUIRED_MSRP_REQUIREMENT_KEYS = (
    "source_registry",
    "official_msrp_ingest",
    "weekly_snapshot",
    "current_price",
    "price_history",
    "price_alerts",
    "review_queue",
    "auto_review_scoring",
    "sales_effectiveness",
    "finance_monthly_lease_subsidy_net",
    "multi_source_reconciliation",
    "dryrun_governance",
    "pipeline_orchestration",
    "frontend_management_views",
)


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
    values = value.split(",") if isinstance(value, str) else value
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        code = str(item).strip().lower()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return tuple(result)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _pipeline_status(repo_root: Path, pipeline_id: str) -> dict[str, Any]:
    path = repo_root / "hermes" / "reports" / "pipeline_status" / f"{pipeline_id}.json"
    payload = _read_json(path)
    if payload is None:
        return {
            "pipelineId": pipeline_id,
            "status": "missing",
            "statusPath": _display_path(path),
        }
    payload.setdefault("pipelineId", pipeline_id)
    payload["statusPath"] = _display_path(path)
    return payload


def _artifact_path(repo_root: Path, artifact_ref: object) -> Path:
    path = Path(str(artifact_ref or "")).expanduser()
    return path if path.is_absolute() else repo_root / path


def _read_msrp_readiness_report(
    repo_root: Path,
    status_record: dict[str, Any],
) -> dict[str, Any] | None:
    artifact_refs = status_record.get("artifactRefs")
    candidates: list[Path] = []
    if isinstance(artifact_refs, list):
        for artifact_ref in artifact_refs:
            path = _artifact_path(repo_root, artifact_ref)
            if path.name == "msrp_readiness_audit.json":
                candidates.insert(0, path)
            elif path.suffix == ".json" and "msrp_readiness_audit" in path.name:
                candidates.append(path)
    candidates.append(repo_root / "hermes" / "reports" / "msrp_readiness_audit.json")

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        payload = _read_json(path)
        if isinstance(payload, dict) and payload.get("schemaVersion") == "msrp_official_price_readiness_v1":
            return payload
    return None


def _requirement_by_key(report: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(report, dict):
        return {}
    requirements = report.get("requirements")
    if not isinstance(requirements, list):
        return {}
    keyed: dict[str, dict[str, Any]] = {}
    for item in requirements:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if key:
            keyed[key] = item
    return keyed


def _msrp_detail_requirements(
    *,
    msrp_report: dict[str, Any] | None,
    msrp_status: dict[str, Any],
) -> list[dict[str, Any]]:
    by_key = _requirement_by_key(msrp_report)
    evidence_root = msrp_status.get("statusPath", "")
    requirements: list[dict[str, Any]] = []
    for key in REQUIRED_MSRP_REQUIREMENT_KEYS:
        detail = by_key.get(key)
        runtime = detail.get("runtime") if isinstance(detail, dict) and isinstance(detail.get("runtime"), dict) else {}
        evidence = []
        if isinstance(detail, dict):
            evidence = [str(item) for item in detail.get("evidence") or [] if item]
        if evidence_root:
            evidence.append(str(evidence_root))
        requirements.append(
            _requirement(
                key=f"msrp_{key}",
                title=str(detail.get("title") if isinstance(detail, dict) else key),
                status=str(detail.get("status") if isinstance(detail, dict) else "missing"),
                evidence=evidence,
                runtime=runtime,
                note=str(
                    detail.get("note")
                    if isinstance(detail, dict)
                    else "Detailed MSRP readiness evidence is missing."
                ),
            )
        )
    return requirements


def _requirement(
    *,
    key: str,
    title: str,
    status: str,
    evidence: list[str],
    runtime: dict[str, Any],
    note: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "evidence": evidence,
        "runtime": runtime,
        "note": note,
    }


def _overall_status(requirements: Sequence[dict[str, Any]]) -> str:
    statuses = {str(item.get("status") or "unknown") for item in requirements}
    if statuses & {"missing", "failed", "not_checked"}:
        return "in_progress"
    if "degraded" in statuses:
        return "degraded"
    return "complete"


def _iter_source_yaml_files(source_draft_dir: Path) -> Iterable[Path]:
    for path in sorted(source_draft_dir.rglob("*.yaml")):
        relative_parts = path.relative_to(source_draft_dir).parts
        if path.name.startswith("_") or any(part.startswith("_") for part in relative_parts):
            continue
        yield path


def _source_draft_coverage(
    source_draft_dir: Path,
    required_countries: Sequence[str],
) -> dict[str, Any]:
    files = list(_iter_source_yaml_files(source_draft_dir))
    by_country = Counter(path.relative_to(source_draft_dir).parts[0] for path in files)
    required = set(required_countries)
    missing_countries = sorted(required - set(by_country))
    todo_files: list[str] = []
    todo_count = 0
    for path in files:
        text = path.read_text(encoding="utf-8", errors="replace")
        count = text.count("TODO_")
        if count:
            todo_count += count
            todo_files.append(_display_path(path))
    return {
        "sourceDraftDir": _display_path(source_draft_dir),
        "sourceDraftCount": len(files),
        "countryCount": len(by_country),
        "filesByCountry": dict(sorted(by_country.items())),
        "missingRequiredCountries": missing_countries,
        "todoPlaceholderCount": todo_count,
        "todoFileCount": len(todo_files),
        "todoSampleFiles": todo_files[:20],
    }


@contextmanager
def _resolve_hostname_to_ip(hostname: str | None, resolve_ip: str | None) -> Iterable[None]:
    if not hostname or not resolve_ip:
        yield
        return
    original_getaddrinfo = socket.getaddrinfo

    def patched_getaddrinfo(host: str, port: Any, *args: Any, **kwargs: Any):
        target_host = resolve_ip if host == hostname else host
        return original_getaddrinfo(target_host, port, *args, **kwargs)

    socket.getaddrinfo = patched_getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


def _fetch_json(
    url: str,
    timeout_seconds: int,
    *,
    resolve_ip: str | None = None,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    try:
        request = Request(url, headers={"User-Agent": "codex-goal-completion-audit"})
        hostname = urlparse(url).hostname
        with _resolve_hostname_to_ip(hostname, resolve_ip):
            with urlopen(request, timeout=max(1, timeout_seconds)) as response:
                payload = json.loads(response.read())
        return payload if isinstance(payload, dict) else {}, None, 200
    except HTTPError as exc:
        detail = exc.read(500).decode("utf-8", errors="replace")
        return None, detail, exc.code
    except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return None, str(exc), None


def _remote_checks(
    remote_api_base: str | None,
    timeout_seconds: int,
    *,
    resolve_ip: str | None = None,
) -> dict[str, Any]:
    if not remote_api_base:
        return {
            "status": "not_checked",
            "note": "Pass --remote-api-base to verify deployed API state.",
        }
    base = remote_api_base.rstrip("/")
    snapshot, snapshot_error, snapshot_code = _fetch_json(
        f"{base}/msrp/current-prices/snapshot",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    progress, progress_error, progress_code = _fetch_json(
        f"{base}/hermes/msrp-country-progress",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    unified, unified_error, unified_code = _fetch_json(
        f"{base}/hermes/pipeline/status/unified_scraping_readiness",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    progress_status = progress.get("status") if isinstance(progress, dict) else {}
    if not isinstance(progress_status, dict):
        progress_status = {}
    passed = (
        snapshot_code == 200
        and isinstance(snapshot, dict)
        and snapshot.get("schemaVersion") == "msrp_current_price_snapshot_v1"
        and progress_code == 200
        and progress_status.get("gateStatus") == "allowed"
        and unified_code == 200
        and isinstance(unified, dict)
        and unified.get("status") == "success"
    )
    return {
        "status": "passed" if passed else "missing",
        "apiBase": base,
        "resolveIp": resolve_ip,
        "snapshot": {
            "httpStatus": snapshot_code,
            "schemaVersion": snapshot.get("schemaVersion") if isinstance(snapshot, dict) else None,
            "snapshotWeek": snapshot.get("snapshotWeek") if isinstance(snapshot, dict) else None,
            "error": snapshot_error,
        },
        "msrpCountryProgress": {
            "httpStatus": progress_code,
            "runId": progress_status.get("runId"),
            "gateStatus": progress_status.get("gateStatus"),
            "overallPassPct": progress_status.get("overallPassPct"),
            "error": progress_error,
        },
        "unifiedScrapingReadiness": {
            "httpStatus": unified_code,
            "status": unified.get("status") if isinstance(unified, dict) else None,
            "readinessStatus": unified.get("readinessStatus") if isinstance(unified, dict) else None,
            "error": unified_error,
        },
    }


def build_goal_completion_report(
    *,
    repo_root: str | Path | None = None,
    source_draft_dir: str | Path = DEFAULT_SOURCE_DRAFT_DIR,
    required_source_countries: Sequence[str] = DEFAULT_REQUIRED_SOURCE_COUNTRIES,
    required_ai_countries: Sequence[str] = DEFAULT_REQUIRED_AI_COUNTRIES,
    remote_api_base: str | None = None,
    remote_resolve_ip: str | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve() if repo_root else REPO_ROOT
    resolved_source_dir = _resolve_path(root, source_draft_dir)
    msrp_status = _pipeline_status(root, "msrp_readiness_audit")
    msrp_report = _read_msrp_readiness_report(root, msrp_status)
    msrp_detail_requirements = _msrp_detail_requirements(
        msrp_report=msrp_report,
        msrp_status=msrp_status,
    )
    ai_status = _pipeline_status(root, "ai_intelligence_enrichment_smoke")
    unified_status = _pipeline_status(root, "unified_scraping_readiness")
    source_coverage = _source_draft_coverage(
        resolved_source_dir,
        required_source_countries,
    )
    remote = _remote_checks(
        remote_api_base,
        timeout_seconds,
        resolve_ip=remote_resolve_ip,
    )

    msrp_missing_keys = [
        item["key"]
        for item in msrp_detail_requirements
        if item.get("status") != "passed"
    ]
    msrp_ready = (
        msrp_status.get("status") == "success"
        and msrp_status.get("readinessStatus") == "passed"
        and not msrp_missing_keys
    )
    ai_news = ai_status.get("news") if isinstance(ai_status.get("news"), dict) else {}
    ai_voc = ai_status.get("voc") if isinstance(ai_status.get("voc"), dict) else {}
    ai_ready = (
        ai_status.get("status") == "success"
        and ai_status.get("smokeStatus") == "ok"
        and int(ai_status.get("requiredCountryCount") or 0) >= len(required_ai_countries)
        and int(ai_news.get("countryCount") or 0) >= len(required_ai_countries)
        and int(ai_voc.get("countryCount") or 0) >= len(required_ai_countries)
    )
    unified_ready = (
        unified_status.get("status") == "success"
        and unified_status.get("readinessStatus") == "passed"
        and unified_status.get("contractStatus") == "ok"
        and unified_status.get("stageStatus") == "ok"
    )
    source_status = "passed"
    if source_coverage["missingRequiredCountries"]:
        source_status = "missing"
    elif source_coverage["todoPlaceholderCount"] > 0:
        source_status = "degraded"

    requirements = [
        _requirement(
            key="msrp_official_price_p0",
            title="MSRP official price local P0 chain",
            status="passed" if msrp_ready else "missing",
            evidence=[msrp_status.get("statusPath", "")],
            runtime=msrp_status,
            note="Aggregate gate: every detailed MSRP readiness requirement below must be passed.",
        ),
        *msrp_detail_requirements,
        _requirement(
            key="ai_news_voc_15_country_smoke",
            title="AI News/VOC 15-country enrichment smoke",
            status="passed" if ai_ready else "missing",
            evidence=[ai_status.get("statusPath", "")],
            runtime=ai_status,
            note="Network-free smoke proves configured sources can feed translation/entity/sentiment/pain point/evidence/digest contracts.",
        ),
        _requirement(
            key="unified_scraping_contract_and_stage",
            title="Unified ScrapeJob contract and stage smoke",
            status="passed" if unified_ready else "missing",
            evidence=[unified_status.get("statusPath", "")],
            runtime=unified_status,
            note="Covers MSRP/news/VOC/policy/incentive/spec job mapping and fixture Fetcher->Extractor->Normalizer->Sink stages.",
        ),
        _requirement(
            key="msrp_21_country_source_draft_coverage",
            title="MSRP 21-country SUV Top30 source draft coverage",
            status=source_status,
            evidence=[source_coverage["sourceDraftDir"]],
            runtime=source_coverage,
            note="Full PRD completion still requires eliminating TODO placeholders and validating real source extraction across all countries.",
        ),
        _requirement(
            key="production_deployment_state",
            title="Production deployment reflects current readiness",
            status=str(remote.get("status") or "not_checked"),
            evidence=[remote.get("apiBase", "")] if remote.get("apiBase") else [],
            runtime=remote,
            note="Production is complete only when deployed API exposes current snapshot, allowed dryrun gate, and unified readiness success.",
        ),
    ]
    status_counts = dict(sorted(Counter(item["status"] for item in requirements).items()))
    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": _overall_status(requirements),
        "generatedAtUtc": _utc_now_iso(),
        "summary": {
            "requirementCount": len(requirements),
            "statusCounts": status_counts,
            "localP0Ready": msrp_ready and ai_ready and unified_ready,
            "msrpDetailedRequirementCount": len(msrp_detail_requirements),
            "msrpDetailedPassedCount": sum(
                1 for item in msrp_detail_requirements
                if item.get("status") == "passed"
            ),
            "msrpMissingRequirementKeys": msrp_missing_keys,
            "sourceDraftTodoPlaceholderCount": source_coverage["todoPlaceholderCount"],
            "sourceDraftCountryCount": source_coverage["countryCount"],
            "productionStatus": remote.get("status"),
        },
        "requirements": requirements,
    }


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    lines = [
        "# JATO MSRP / AI / Unified Goal Completion Audit",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        f"**Local P0 ready:** {summary.get('localP0Ready', False)}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Requirements | {summary.get('requirementCount', 0)} |",
        f"| Passed | {(summary.get('statusCounts') or {}).get('passed', 0)} |",
        f"| Degraded | {(summary.get('statusCounts') or {}).get('degraded', 0)} |",
        f"| Missing | {(summary.get('statusCounts') or {}).get('missing', 0)} |",
        f"| Not checked | {(summary.get('statusCounts') or {}).get('not_checked', 0)} |",
        f"| MSRP detailed passed | {summary.get('msrpDetailedPassedCount', 0)} / {summary.get('msrpDetailedRequirementCount', 0)} |",
        f"| Source draft countries | {summary.get('sourceDraftCountryCount', 0)} |",
        f"| Source TODO placeholders | {summary.get('sourceDraftTodoPlaceholderCount', 0)} |",
        "",
        "## Requirements",
        "",
        "| Key | Status | Note |",
        "|---|---|---|",
    ]
    for item in report.get("requirements") or []:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(item.get("key")),
                    _markdown_cell(item.get("status")),
                    _markdown_cell(item.get("note")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "goal_completion_audit.json"
    latest_md = output_root / "goal_completion_audit.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"goal_completion_audit_{suffix}.json"
    hist_md = output_root / f"goal_completion_audit_{suffix}.md"
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
    status = str(report.get("status") or "in_progress")
    pipeline_status = "success" if status == "complete" else "degraded" if status == "degraded" else "failed"
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    status_counts = summary.get("statusCounts") if isinstance(summary.get("statusCounts"), dict) else {}
    failed_count = int(status_counts.get("missing") or 0) + int(status_counts.get("not_checked") or 0)
    warning_count = int(status_counts.get("degraded") or 0)
    return write_pipeline_status(
        pipeline_id=PIPELINE_ID,
        status=pipeline_status,
        started_at=report.get("generatedAtUtc"),
        finished_at=_utc_now_iso(),
        exit_code=0 if status in {"complete", "degraded"} else 1,
        records_processed=int(summary.get("requirementCount") or 0),
        failed_count=failed_count,
        warning_count=warning_count,
        artifact_refs=list(artifact_refs),
        source=PIPELINE_ID,
        message=(
            f"Goal completion={status}; localP0={summary.get('localP0Ready')}, "
            f"sourceTODO={summary.get('sourceDraftTodoPlaceholderCount')}, "
            f"production={summary.get('productionStatus')}."
        ),
        extra={
            "goalCompletionStatus": status,
            "localP0Ready": summary.get("localP0Ready", False),
            "statusCounts": status_counts,
            "msrpMissingRequirementKeys": summary.get("msrpMissingRequirementKeys", []),
            "sourceDraftTodoPlaceholderCount": summary.get("sourceDraftTodoPlaceholderCount", 0),
            "productionStatus": summary.get("productionStatus"),
        },
        repo_root=REPO_ROOT,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit full goal completion across MSRP, AI News/VOC, and Unified Scraping.",
    )
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--source-draft-dir", default=DEFAULT_SOURCE_DRAFT_DIR)
    parser.add_argument("--required-source-countries", default=",".join(DEFAULT_REQUIRED_SOURCE_COUNTRIES))
    parser.add_argument("--required-ai-countries", default=",".join(DEFAULT_REQUIRED_AI_COUNTRIES))
    parser.add_argument("--remote-api-base", default=None)
    parser.add_argument(
        "--remote-resolve-ip",
        default=None,
        help="Resolve the remote API hostname to this IP while keeping the URL host for TLS/SNI.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--write-status", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless status is complete.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_goal_completion_report(
        repo_root=args.repo_root,
        source_draft_dir=args.source_draft_dir,
        required_source_countries=_csv_arg(args.required_source_countries),
        required_ai_countries=_csv_arg(args.required_ai_countries),
        remote_api_base=args.remote_api_base,
        remote_resolve_ip=args.remote_resolve_ip,
        timeout_seconds=max(1, int(args.timeout_seconds)),
    )
    artifacts: dict[str, str] = {}
    if args.out_dir:
        artifacts = write_outputs(report, args.out_dir)
        report["reportArtifacts"] = artifacts
    if args.write_status:
        status_record = write_status_record(report, artifact_refs=list(artifacts.values()))
        if status_record is not None:
            report["pipelineStatus"] = status_record
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.strict and report["status"] != "complete":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
