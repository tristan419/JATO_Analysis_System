#!/usr/bin/env python3
"""Read-only readiness audit for the MSRP official price enrichment PRD."""

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
SCRIPTS_ROOT = REPO_ROOT / "03_Scripts"
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
DRYRUN_ARTIFACTS_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
DRYRUN_REPORT_PATH = DRYRUN_ARTIFACTS_DIR / "dryrun_report.json"
DRYRUN_RUNS_INDEX_PATH = DRYRUN_ARTIFACTS_DIR / "dryrun_runs_index.json"
SOURCE_REPAIR_BACKLOG_PATH = DRYRUN_ARTIFACTS_DIR / "msrp_source_repair_backlog.json"
PRICE_ALERT_REVIEW_QUEUE_PATH = DRYRUN_ARTIFACTS_DIR / "msrp_price_alert_review_queue.json"
MSRP_SOURCE_COUNTRY_CODES = {
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
}
for path in (SCRIPT_DIR, SCRIPTS_ROOT, HERMES_SCRIPT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from engineering_config_source_sync import (  # noqa: E402
    DEFAULT_SPEC_BATCH,
    build_source_sync_report,
)
from msrp_workflow_smoke import ApiClient, DEFAULT_API_BASE, SmokeFailure  # noqa: E402

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - import is optional for pure unit tests.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "msrp_official_price_readiness_v1"
WRITE_ROLE_LEVELS = {
    "viewer": 1,
    "order_filler": 1,
    "editor": 2,
    "admin": 3,
    "developer": 3,
}
MIN_WRITE_ROLE_LEVEL = WRITE_ROLE_LEVELS["editor"]

TEST_EVIDENCE = {
    "workflowSmoke": "03_Scripts/tests/test_msrp_workflow_smoke.py",
    "workflowService": "06_AppPlatform/backend/tests/unit/test_msrp_workflow_service.py",
    "monitoringService": "06_AppPlatform/backend/tests/unit/test_msrp_monitoring_service.py",
    "snapshotScript": "03_Scripts/tests/test_hermes_msrp_current_price_snapshot.py",
    "priceAlertReviewQueue": "03_Scripts/tests/test_msrp_price_alert_review_queue.py",
    "frontendApi": "06_AppPlatform/frontend/src/tests/unit/dataManagementApi.test.ts",
    "pipelineWrapper": "03_Scripts/tests/test_run_msrp_pipeline.py",
    "configSourceSync": "03_Scripts/tests/test_engineering_config_source_sync.py",
    "scraperValidation": "06_AppPlatform/backend/tests/unit/test_scraper_validation.py",
    "httpJsonExtractor": "07_ScrapingToolkit/tests/test_http_json.py",
    "scraplingExtractor": "07_ScrapingToolkit/tests/test_scrapling_web.py",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _repo_path(value: str) -> Path:
    return REPO_ROOT / value


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


def _test_file_has(path_key: str, *needles: str) -> bool:
    path = _repo_path(TEST_EVIDENCE[path_key])
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    return all(needle in text for needle in needles)


def _status(
    passed: bool,
    *,
    degraded: bool = False,
    unavailable: bool = False,
) -> str:
    if passed:
        return "passed"
    if unavailable:
        return "missing"
    if degraded:
        return "degraded"
    return "missing"


def _count_from_payload(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    items = payload.get("items")
    if isinstance(items, list):
        return len(items)
    return None


def _summary_count(payload: dict[str, Any], key: str) -> int | None:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return None
    value = summary.get(key)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _nested_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_path_from_ref(path_ref: Any) -> Path | None:
    if not path_ref:
        return None
    path = Path(str(path_ref))
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


def _load_indexed_dryrun_report(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("runId") or "")
    candidate_paths: list[Path] = []
    artifact_path = _artifact_path_from_ref(run.get("artifactPath"))
    if artifact_path:
        candidate_paths.append(artifact_path)
    if run_id:
        candidate_paths.append(DRYRUN_ARTIFACTS_DIR / f"dryrun_report_{run_id}.json")

    seen: set[Path] = set()
    for path in candidate_paths:
        if path in seen:
            continue
        seen.add(path)
        report = _read_json_file(path)
        if report.get("schemaVersion") == "msrp_dryrun_report_v3":
            return report
    return {}


def _country_codes_from_run(run: dict[str, Any]) -> list[str]:
    batch = str(run.get("batch") or "").strip().lower()
    if not batch:
        return []
    tokens = batch.replace("+", ",").replace("/", ",").split(",")
    return [
        token.strip()
        for token in tokens
        if token.strip() in MSRP_SOURCE_COUNTRY_CODES
    ]


def _dryrun_history_from_artifacts() -> dict[str, Any]:
    index = _read_json_file(DRYRUN_RUNS_INDEX_PATH)
    runs = index.get("runs") if isinstance(index.get("runs"), list) else []
    if not runs:
        return {}
    latest_run_id = str(index.get("latestRunId") or runs[0].get("runId") or "")
    return {
        "schemaVersion": index.get("schemaVersion"),
        "latestRunId": latest_run_id,
        "updatedAt": index.get("updatedAt"),
        "runs": runs,
        "source": _display_path(DRYRUN_RUNS_INDEX_PATH),
    }


def _run_recency_key(run: dict[str, Any]) -> tuple[str, str]:
    return (
        str(run.get("finishedAt") or run.get("startedAt") or ""),
        str(run.get("runId") or ""),
    )


def _is_source_filtered_run(run: dict[str, Any]) -> bool:
    source_filter = run.get("sourceFilter")
    return bool(
        run.get("isSourceFiltered")
        or (isinstance(source_filter, list) and source_filter)
    )


def _updates_latest_artifact(run: dict[str, Any]) -> bool:
    if run.get("updatesLatestArtifact") is False:
        return False
    return not _is_source_filtered_run(run)


def _is_diagnostic_dryrun_run(run: dict[str, Any]) -> bool:
    return not _updates_latest_artifact(run)


def _float_value(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _country_latest_row_from_v3(
    *,
    run: dict[str, Any],
    report: dict[str, Any],
    country: dict[str, Any],
) -> dict[str, Any] | None:
    country_code = str(country.get("countryCode") or "").strip().lower()
    if country_code not in MSRP_SOURCE_COUNTRY_CODES:
        return None
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    gate_threshold = _float_value(
        run.get("gateThreshold", summary.get("gateThreshold")),
        70.0,
    )
    pass_pct = _float_value(country.get("passPct"), 0.0)
    return {
        "countryCode": country_code,
        "runId": report.get("runId") or run.get("runId"),
        "batch": report.get("batch") or run.get("batch"),
        "status": country.get("status") or run.get("status"),
        "gateStatus": "allowed" if pass_pct >= gate_threshold else "blocked",
        "gateThreshold": gate_threshold,
        "passPct": pass_pct,
        "total": _int_value(country.get("total")),
        "pass": _int_value(country.get("pass")),
        "empty": _int_value(country.get("empty")),
        "fail": _int_value(country.get("fail")) + _int_value(country.get("errors")),
        "errors": _int_value(country.get("errors")),
        "finishedAt": (
            run.get("finishedAt")
            or report.get("generatedAt")
            or run.get("startedAt")
        ),
        "artifactPath": run.get("artifactPath"),
    }


def _country_latest_rows_from_v3_report(
    run: dict[str, Any],
    report: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for country in report.get("countriesDetail") or []:
        if not isinstance(country, dict):
            continue
        row = _country_latest_row_from_v3(
            run=run,
            report=report,
            country=country,
        )
        if row:
            rows.append(row)
    return rows


def _country_latest_rows_from_run_summary(run: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for country_code in _country_codes_from_run(run):
        rows.append({
            "countryCode": country_code,
            "runId": run.get("runId"),
            "batch": run.get("batch"),
            "status": run.get("status"),
            "gateStatus": run.get("gateStatus"),
            "gateThreshold": run.get("gateThreshold"),
            "passPct": run.get("passPct"),
            "total": run.get("total"),
            "pass": run.get("pass"),
            "empty": run.get("empty"),
            "fail": run.get("fail"),
            "errors": run.get("errors"),
            "finishedAt": run.get("finishedAt") or run.get("startedAt"),
            "artifactPath": run.get("artifactPath"),
        })
    return rows


def _is_stable_country_latest(row: dict[str, Any]) -> bool:
    return _float_value(row.get("passPct"), 0.0) >= _float_value(
        row.get("gateThreshold"),
        70.0,
    )


def _all_country_latest_from_dryrun_runs(
    runs: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    stable_by_country: dict[str, dict[str, Any]] = {}
    fallback_by_country: dict[str, dict[str, Any]] = {}
    for run in sorted(runs, key=_run_recency_key, reverse=True):
        if not isinstance(run, dict):
            continue
        if _is_diagnostic_dryrun_run(run):
            continue
        report = _load_indexed_dryrun_report(run)
        country_rows = (
            _country_latest_rows_from_v3_report(run, report)
            if report
            else _country_latest_rows_from_run_summary(run)
        )
        for row in country_rows:
            country_code = str(row.get("countryCode") or "")
            if not country_code:
                continue
            fallback_by_country.setdefault(country_code, row)
            if country_code not in stable_by_country and _is_stable_country_latest(row):
                stable_by_country[country_code] = row

    countries_by_code = {
        country_code: stable_by_country.get(country_code, fallback)
        for country_code, fallback in fallback_by_country.items()
    }
    return [countries_by_code[key] for key in sorted(countries_by_code)]


def _stable_coverage_from_artifact_latest(
    all_countries_latest: Sequence[dict[str, Any]],
    latest_run_id: str,
) -> dict[str, Any]:
    country_count = len(all_countries_latest)
    source_count = sum(int(item.get("total") or 0) for item in all_countries_latest)
    ready_source_count = sum(int(item.get("pass") or 0) for item in all_countries_latest)
    ready_countries = [
        str(item.get("countryCode"))
        for item in all_countries_latest
        if str(item.get("gateStatus") or "").lower() == "allowed"
    ]
    blocked_countries = [
        str(item.get("countryCode"))
        for item in all_countries_latest
        if str(item.get("gateStatus") or "").lower() != "allowed"
    ]
    source_pass_rate = (
        round(ready_source_count / source_count * 100, 1)
        if source_count
        else 0.0
    )
    return {
        "latestRunId": latest_run_id,
        "activeRunId": latest_run_id,
        "countryCount": country_count,
        "readyCountryCount": len(ready_countries),
        "blockedCountryCount": len(blocked_countries),
        "readyCountries": ready_countries,
        "blockedCountries": blocked_countries,
        "sourceRowsObserved": 0,
        "sourceCount": source_count,
        "readySourceCount": ready_source_count,
        "sourcePassRate": source_pass_rate,
        "stablePassRate": source_pass_rate,
        "probeDiffersFromStableRun": False,
    }


def _dryrun_country_progress_from_artifacts(
    artifact_history: dict[str, Any],
) -> dict[str, Any]:
    runs = artifact_history.get("runs") if isinstance(artifact_history.get("runs"), list) else []
    if not runs:
        return {}
    latest_run = runs[0] if isinstance(runs[0], dict) else {}
    latest_run_id = str(artifact_history.get("latestRunId") or latest_run.get("runId") or "")
    report = _read_json_file(DRYRUN_REPORT_PATH)
    summary = _nested_dict(report, "summary")
    gate_status = summary.get("gateStatus") or latest_run.get("gateStatus") or "blocked"
    pass_pct = summary.get("passPct", latest_run.get("passPct"))
    all_countries_latest = _all_country_latest_from_dryrun_runs(runs)
    stable_coverage = _stable_coverage_from_artifact_latest(
        all_countries_latest,
        latest_run_id,
    )
    return {
        "overall": "ok" if str(gate_status).lower() == "allowed" else "critical",
        "status": {
            "runId": report.get("runId") or latest_run_id,
            "schemaVersion": report.get("schemaVersion"),
            "overallPassPct": pass_pct,
            "gateThreshold": summary.get("gateThreshold", latest_run.get("gateThreshold")),
            "gateStatus": gate_status,
            "expectedCountries": report.get("expectedCountries", []),
            "observedCountries": report.get("observedCountries", []),
            "missingCountries": report.get("missingCountries", []),
            "duplicateCountries": report.get("duplicateCountries", []),
            "stableLatestRunId": stable_coverage.get("latestRunId"),
            "activeRunId": stable_coverage.get("activeRunId"),
        },
        "allCountriesLatest": all_countries_latest,
        "stableCoverage": stable_coverage,
        "sourceRepairBacklog": _read_json_file(SOURCE_REPAIR_BACKLOG_PATH),
        "artifactFallback": {
            "historyPath": _display_path(DRYRUN_RUNS_INDEX_PATH),
            "reportPath": _display_path(DRYRUN_REPORT_PATH),
        },
    }


def _should_use_dryrun_artifact_fallback(
    *,
    api_history: dict[str, Any],
    api_progress: dict[str, Any],
    artifact_history: dict[str, Any],
) -> bool:
    artifact_latest = str(artifact_history.get("latestRunId") or "")
    if not artifact_latest:
        return False
    api_latest = str(api_history.get("latestRunId") or "")
    api_all_countries = api_progress.get("allCountriesLatest")
    if not api_latest or api_latest != artifact_latest:
        return True
    return not isinstance(api_all_countries, list) or not api_all_countries


def _safe_get(
    client: ApiClient,
    path: str,
    *,
    query: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str | None]:
    try:
        return client.request_json("GET", path, query=query), None
    except SmokeFailure as exc:
        return {}, str(exc)


def _role_level(payload: dict[str, Any]) -> tuple[str, int]:
    role = str(payload.get("role") or "").strip().lower()
    return role or "unknown", WRITE_ROLE_LEVELS.get(role, 0)


def _requirement(
    *,
    key: str,
    title: str,
    status: str,
    runtime: dict[str, Any],
    evidence: list[str],
    note: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "runtime": runtime,
        "evidence": evidence,
        "note": note,
    }


def _overall_status(requirements: Sequence[dict[str, Any]]) -> str:
    statuses = {str(item.get("status")) for item in requirements}
    if "missing" in statuses:
        return "missing"
    if "degraded" in statuses:
        return "degraded"
    return "passed"


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    status_counts = (
        summary.get("statusCounts")
        if isinstance(summary.get("statusCounts"), dict)
        else {}
    )
    runtime_counts = (
        summary.get("runtimeCounts")
        if isinstance(summary.get("runtimeCounts"), dict)
        else {}
    )
    requirements = [
        item for item in report.get("requirements") or []
        if isinstance(item, dict)
    ]
    lines: list[str] = [
        "# MSRP Official Price Readiness",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        f"**API base:** {report.get('apiBase', '-')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Requirements | {summary.get('requirementCount', 0)} |",
        f"| Passed | {status_counts.get('passed', 0)} |",
        f"| Degraded | {status_counts.get('degraded', 0)} |",
        f"| Missing | {status_counts.get('missing', 0)} |",
        f"| Current prices | {runtime_counts.get('currentPriceCount', 0)} |",
        f"| Price history rows | {runtime_counts.get('priceHistoryRows', 0)} |",
        f"| Finance observations | {runtime_counts.get('financeObservationCount', 0)} |",
        f"| Official config sources | {runtime_counts.get('officialConfigSourceCount', 0)} |",
        f"| Reconciliation conflicts | {runtime_counts.get('reconciliationConflictGroups', 0)} |",
        "",
        "## Requirements",
        "",
        "| Key | Status | Runtime | Evidence |",
        "|---|---|---|---|",
    ]
    for item in requirements:
        runtime = item.get("runtime") if isinstance(item.get("runtime"), dict) else {}
        runtime_preview = ", ".join(
            f"{key}={value}"
            for key, value in list(runtime.items())[:4]
            if value not in (None, "", [], {})
        )
        evidence = "; ".join(str(value) for value in item.get("evidence") or [])
        lines.append(
            "| "
            + " | ".join([
                _markdown_cell(item.get("key")),
                _markdown_cell(item.get("status")),
                _markdown_cell(runtime_preview or "-"),
                _markdown_cell(evidence),
            ])
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "msrp_readiness_audit.json"
    latest_md = output_root / "msrp_readiness_audit.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"msrp_readiness_audit_{suffix}.json"
    hist_md = output_root / f"msrp_readiness_audit_{suffix}.md"
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
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    status_counts = (
        summary.get("statusCounts")
        if isinstance(summary.get("statusCounts"), dict)
        else {}
    )
    runtime_counts = (
        summary.get("runtimeCounts")
        if isinstance(summary.get("runtimeCounts"), dict)
        else {}
    )
    missing_count = int(status_counts.get("missing") or 0)
    degraded_count = int(status_counts.get("degraded") or 0)
    readiness_status = str(report.get("status") or "missing")
    pipeline_status = (
        "success"
        if readiness_status == "passed"
        else "degraded"
        if readiness_status == "degraded"
        else "failed"
    )
    return write_pipeline_status(
        pipeline_id="msrp_readiness_audit",
        status=pipeline_status,
        started_at=started_at,
        finished_at=_utc_now_iso(),
        exit_code=0 if readiness_status in {"passed", "degraded"} else 1,
        records_processed=int(summary.get("requirementCount") or 0),
        failed_count=missing_count,
        warning_count=degraded_count,
        artifact_refs=list(artifact_refs),
        source="msrp_readiness_audit",
        message=(
            f"MSRP readiness={readiness_status}; "
            f"passed={status_counts.get('passed', 0)}, "
            f"degraded={degraded_count}, missing={missing_count}."
        ),
        extra={
            "readinessStatus": readiness_status,
            "statusCounts": status_counts,
            "runtimeCounts": runtime_counts,
        },
        repo_root=REPO_ROOT,
    )


def build_readiness_report(
    *,
    client: ApiClient,
    filters: dict[str, Any],
) -> dict[str, Any]:
    query = {key: value for key, value in filters.items() if value not in (None, "")}
    limit_query = {**query, "limit": 50}
    snapshot_query = {**query, "limit": 50, "threshold_pct": filters.get("threshold_pct", 3.0)}

    sources, sources_error = _safe_get(client, "/msrp/sources", query={"limit": 50})
    observations, observations_error = _safe_get(
        client,
        "/msrp/sources/observations",
        query=limit_query,
    )
    current_prices, current_prices_error = _safe_get(
        client,
        "/msrp/current-prices",
        query=limit_query,
    )
    history, history_error = _safe_get(
        client,
        "/msrp/price-history",
        query=limit_query,
    )
    alerts, alerts_error = _safe_get(
        client,
        "/msrp/current-prices/alerts",
        query=snapshot_query,
    )
    monitoring, monitoring_error = _safe_get(
        client,
        "/msrp/monitoring/events",
        query={**query, "window_days": 365, "threshold_pct": filters.get("threshold_pct", 3.0), "limit": 500},
    )
    snapshot, snapshot_error = _safe_get(
        client,
        "/msrp/current-prices/snapshot",
        query=snapshot_query,
    )
    finance, finance_error = _safe_get(
        client,
        "/msrp/finance-observations",
        query={
            **limit_query,
            "has_monthly_payment": True,
            "has_subsidy": True,
            "has_net_price_after_subsidy": True,
        },
    )
    auth_me, auth_error = _safe_get(client, "/auth/me")
    reconciliation, reconciliation_error = _safe_get(
        client,
        "/msrp/reconciliation",
        query={**limit_query, "threshold_pct": filters.get("reconciliation_threshold_pct", 1.0)},
    )
    effectiveness, effectiveness_error = _safe_get(
        client,
        "/msrp/effectiveness",
        query={
            **limit_query,
            "threshold_pct": filters.get("threshold_pct", 3.0),
            "baseline_window_months": 3,
            "post_window_months": 3,
            "post_lag_months": 1,
            "min_months": 1,
        },
    )
    price_alert_review_queue = _read_json_file(PRICE_ALERT_REVIEW_QUEUE_PATH)
    country_progress, country_progress_error = _safe_get(
        client,
        "/hermes/msrp-country-progress",
    )
    dryrun_history, dryrun_history_error = _safe_get(
        client,
        "/hermes/msrp-dryrun-history",
    )
    artifact_dryrun_history = _dryrun_history_from_artifacts()
    dryrun_artifact_fallback_used = _should_use_dryrun_artifact_fallback(
        api_history=dryrun_history,
        api_progress=country_progress,
        artifact_history=artifact_dryrun_history,
    )
    if dryrun_artifact_fallback_used:
        artifact_country_progress = _dryrun_country_progress_from_artifacts(
            artifact_dryrun_history
        )
        if artifact_country_progress:
            country_progress = artifact_country_progress
            country_progress_error = None
        if artifact_dryrun_history:
            dryrun_history = artifact_dryrun_history
            dryrun_history_error = None
    try:
        config_source_sync = build_source_sync_report(
            repo_root=REPO_ROOT,
            spec_batch=DEFAULT_SPEC_BATCH,
            sample_per_country=1,
            run_stage_smoke=True,
        )
        config_source_sync_error = None
    except Exception as exc:  # noqa: BLE001 - readiness reports local config blockers.
        config_source_sync = {}
        config_source_sync_error = str(exc)

    source_count = _count_from_payload(sources, "total", "rows")
    observation_count = _count_from_payload(observations, "total", "rows")
    current_price_count = _count_from_payload(current_prices, "total", "rows")
    history_count = _count_from_payload(history, "rows", "total")
    alert_count = _count_from_payload(alerts, "total", "rows")
    monitoring_summary = _nested_dict(monitoring, "summary")
    monitoring_event_count = int(monitoring_summary.get("eventCount") or 0)
    monitoring_timeline_count = int(monitoring_summary.get("timelineEventCount") or 0)
    monitoring_source_risk_count = int(monitoring_summary.get("sourceRiskCount") or 0)
    monitoring_review_required_count = int(monitoring_summary.get("reviewRequiredCount") or 0)
    monitoring_warnings = monitoring.get("warnings") if isinstance(monitoring.get("warnings"), list) else []
    snapshot_current_count = _summary_count(snapshot, "currentPriceCount")
    snapshot_alert_count = _summary_count(snapshot, "priceAlertCount")
    finance_count = _count_from_payload(finance, "total", "rows")
    reconciliation_summary = _nested_dict(reconciliation, "summary")
    reconciliation_status_counts = _nested_dict(reconciliation_summary, "statusCounts")
    conflict_count = int(reconciliation_status_counts.get("conflict") or 0)
    single_source_count = int(reconciliation_status_counts.get("single_source") or 0)
    effectiveness_summary = _nested_dict(effectiveness, "summary")
    effectiveness_labels = _nested_dict(effectiveness_summary, "labelCounts")
    price_alert_review_summary = _nested_dict(price_alert_review_queue, "summary")
    price_alert_review_schema_ok = (
        price_alert_review_queue.get("schemaVersion")
        == "msrp_price_alert_review_queue_v1"
    )
    price_alert_review_case_count = int(
        price_alert_review_summary.get("totalCases") or 0
    )
    price_alert_review_follow_up_count = int(
        price_alert_review_summary.get("effectivenessFollowUpCount") or 0
    )
    price_alert_review_linked_count = int(
        price_alert_review_summary.get("effectivenessLinkedCount") or 0
    )
    price_alert_review_missing_count = int(
        price_alert_review_summary.get("effectivenessMissingCount") or 0
    )
    dryrun_status = _nested_dict(country_progress, "status")
    dryrun_runs = dryrun_history.get("runs") if isinstance(dryrun_history.get("runs"), list) else []
    all_countries_latest = (
        country_progress.get("allCountriesLatest")
        if isinstance(country_progress.get("allCountriesLatest"), list)
        else []
    )
    stable_coverage = _nested_dict(country_progress, "stableCoverage")
    source_repair_backlog = _nested_dict(country_progress, "sourceRepairBacklog")
    source_repair_issue_count = int(
        source_repair_backlog.get("sourceRepairIssueCount")
        or source_repair_backlog.get("totalIssueCount")
        or 0
    )
    transient_recheck_count = int(
        source_repair_backlog.get("transientRegressionCount") or 0
    )
    dryrun_pass_pct = dryrun_status.get("overallPassPct")
    dryrun_gate = dryrun_status.get("gateStatus")
    config_source_summary = _nested_dict(config_source_sync, "summary")
    config_source_warehouse = _nested_dict(config_source_sync, "warehouseContract")
    config_source_landing = _nested_dict(config_source_sync, "warehouseLanding")
    config_source_status = str(config_source_sync.get("status") or "failed")
    config_source_count = int(config_source_summary.get("sourceCount") or 0)
    config_country_count = int(config_source_summary.get("countryCount") or 0)
    config_missing_countries = config_source_summary.get("missingRequiredCountries")
    if not isinstance(config_missing_countries, list):
        config_missing_countries = []
    write_role, write_role_level = _role_level(auth_me)
    write_auth_ok = write_role_level >= MIN_WRITE_ROLE_LEVEL
    write_auth_reason = (
        None
        if write_auth_ok
        else "auth_preflight_request_failed"
        if auth_error is not None
        else "write_role_required"
    )

    smoke_covers_full_contract = _test_file_has(
        "workflowSmoke",
        "test_run_smoke_exercises_full_contract_with_fake_client",
        "finance_observations",
        "review_cases_queued",
        "effectiveness_labels",
    )
    service_covers_effectiveness_positive = _test_file_has(
        "workflowService",
        "test_build_price_sales_effectiveness_compares_sales_windows",
        "\"positive\": 1",
    )
    service_covers_reconciliation = _test_file_has(
        "workflowService",
        "test_build_multi_source_reconciliation_flags_source_conflict",
        "conflict",
    )
    service_covers_auto_review_scoring = _test_file_has(
        "workflowService",
        "test_msrp_auto_review_score_uses_weighted_rules_and_model_guidance",
        "msrp_auto_review_score_v1",
        "not_recommended_until_labeled_corpus",
    )
    service_covers_monitoring_events = _test_file_has(
        "monitoringService",
        "test_build_msrp_monitoring_events_groups_price_changes_with_evidence",
        "test_build_msrp_monitoring_events_groups_multi_country_sync_and_outlier",
        "msrp_monitoring_events_v1",
    )
    script_covers_full_pipeline = _test_file_has(
        "pipelineWrapper",
        "test_pipeline_fails_before_dryrun_when_config_source_sync_fails",
        "engineering_config_source_sync",
        "test_pipeline_runs_ingest_when_dryrun_gate_is_allowed",
        "test_pipeline_skips_ingest_when_dryrun_gate_blocks",
        "test_pipeline_fails_when_post_audit_fails",
        "unified_scraping_readiness",
        "goal_completion_audit",
        "msrp_pipeline",
    )
    snapshot_script_covered = _test_file_has(
        "snapshotScript",
        "msrp_current_price_snapshot_v1",
        "test_run_writes_degraded_status_for_high_priority_alert",
    )
    snapshot_archive_covers_full_prd = _test_file_has(
        "snapshotScript",
        "priceSalesEffectiveness",
        "multiSourceReconciliation",
        "financeObservations",
        "test_run_degrades_for_reconciliation_conflicts",
    )
    price_alert_review_queue_covered = _test_file_has(
        "priceAlertReviewQueue",
        "msrp_price_alert_review_queue_v1",
        "salesEffectivenessAvailable",
        "effectivenessLinkedCount",
    )
    frontend_finance_reconciliation_covered = _test_file_has(
        "frontendApi",
        "listMsrpFinanceObservations",
        "listMsrpReconciliation",
        "queueMsrpReconciliationReviewCases",
    )
    finance_validation_covered = _test_file_has(
        "scraperValidation",
        "test_monthly_lease_amount_passes_finance_semantics",
        "test_monthly_amount_still_rejected_without_finance_semantics",
        "source_price_semantics",
    )
    finance_extractor_context_covered = (
        _test_file_has(
            "httpJsonExtractor",
            "test_http_json_adds_pricing_context_from_profile",
            "test_config_loader_builds_http_json_pricing_context_profile",
            "lease_monthly",
        )
        and _test_file_has(
            "scraplingExtractor",
            "test_build_observation_adds_pricing_context_from_profile",
            "test_config_loader_builds_scrapling_pricing_context_profile",
            "lease_monthly",
        )
    )
    config_source_sync_covered = _test_file_has(
        "configSourceSync",
        "engineering_config_source_sync_v1",
        "SpecFeatureObservation",
        "engineering_config.vehicle_trims",
        "engineering_config.trim_feature_values",
        "spec_feature_observation_to_engineering_config_landing_v1",
    )

    requirements = [
        _requirement(
            key="source_registry",
            title="Official source registry",
            status=_status(
                bool(source_count and source_count > 0),
                degraded=sources_error is None,
                unavailable=sources_error is not None,
            ),
            runtime={"sourceCount": source_count, "error": sources_error},
            evidence=["GET /msrp/sources", TEST_EVIDENCE["workflowSmoke"]],
            note="Maintains country/brand official source metadata.",
        ),
        _requirement(
            key="official_msrp_ingest_auth",
            title="Official MSRP write auth preflight",
            status=_status(
                write_auth_ok,
                unavailable=auth_error is not None or not write_auth_ok,
            ),
            runtime={
                "authStatus": "ok" if write_auth_ok else "auth_failed",
                "role": write_role,
                "requiredRole": "editor",
                "user": auth_me.get("username") or auth_me.get("name"),
                "reason": write_auth_reason,
                "error": auth_error,
            },
            evidence=[
                "GET /auth/me",
                "07_ScrapingToolkit/jato_scraper/runner.py",
            ],
            note="Confirms the configured token can write ingest mutations before scraper network work begins.",
        ),
        _requirement(
            key="official_msrp_ingest",
            title="Official MSRP ingestion",
            status=_status(
                bool((observation_count and observation_count > 0) or (current_price_count and current_price_count > 0)),
                degraded=bool(dryrun_history and not observations_error),
                unavailable=observations_error is not None and current_prices_error is not None,
            ),
            runtime={
                "observationCount": observation_count,
                "currentPriceCount": current_price_count,
                "dryrunLatestRunId": dryrun_history.get("latestRunId"),
                "dryrunGateStatus": dryrun_gate,
                "dryrunPassPct": dryrun_pass_pct,
                "errors": [item for item in [observations_error, current_prices_error] if item],
            },
            evidence=[
                "GET /msrp/sources/observations",
                "GET /msrp/current-prices",
                TEST_EVIDENCE["workflowSmoke"],
            ],
            note="Confirms official MSRP observations can materialize into current prices.",
        ),
        _requirement(
            key="weekly_snapshot",
            title="Weekly current-price snapshot",
            status=_status(
                snapshot.get("schemaVersion") == "msrp_current_price_snapshot_v1"
                and bool(snapshot.get("snapshotWeek"))
                and snapshot_script_covered
                and snapshot_archive_covers_full_prd,
                degraded=snapshot_error is None,
                unavailable=snapshot_error is not None,
            ),
            runtime={
                "schemaVersion": snapshot.get("schemaVersion"),
                "snapshotWeek": snapshot.get("snapshotWeek"),
                "currentPriceCount": snapshot_current_count,
                "priceAlertCount": snapshot_alert_count,
                "scriptCovered": snapshot_script_covered,
                "archiveIncludesEffectivenessReconciliationFinance": (
                    snapshot_archive_covers_full_prd
                ),
                "error": snapshot_error,
            },
            evidence=[
                "GET /msrp/current-prices/snapshot",
                TEST_EVIDENCE["snapshotScript"],
            ],
            note="Snapshot is the weekly cached fact source for dashboard/Hermes.",
        ),
        _requirement(
            key="current_price",
            title="Current price read model",
            status=_status(
                bool((current_price_count and current_price_count > 0) or (snapshot_current_count and snapshot_current_count > 0)),
                degraded=current_prices_error is None or snapshot_error is None,
                unavailable=current_prices_error is not None and snapshot_error is not None,
            ),
            runtime={
                "currentPriceCount": current_price_count,
                "snapshotCurrentPriceCount": snapshot_current_count,
                "error": current_prices_error,
            },
            evidence=["GET /msrp/current-prices", TEST_EVIDENCE["workflowSmoke"]],
            note="Latest accepted MSRP per country/brand/model/trim/powertrain.",
        ),
        _requirement(
            key="price_history",
            title="Price history periods",
            status=_status(
                bool(history_count and history_count > 0),
                degraded=history_error is None and smoke_covers_full_contract,
                unavailable=history_error is not None,
            ),
            runtime={"priceHistoryRows": history_count, "error": history_error},
            evidence=["GET /msrp/price-history", TEST_EVIDENCE["workflowSmoke"]],
            note="Stores weekly periods instead of overwriting old prices.",
        ),
        _requirement(
            key="price_alerts",
            title="Price change alerts",
            status=_status(
                bool((alert_count and alert_count > 0) or (snapshot_alert_count and snapshot_alert_count > 0)),
                degraded=alerts_error is None and smoke_covers_full_contract,
                unavailable=alerts_error is not None and snapshot_error is not None,
            ),
            runtime={
                "alertCount": alert_count,
                "snapshotAlertCount": snapshot_alert_count,
                "error": alerts_error,
            },
            evidence=[
                "GET /msrp/current-prices/alerts",
                TEST_EVIDENCE["workflowSmoke"],
                TEST_EVIDENCE["snapshotScript"],
            ],
            note="Detects price movement and threshold severity.",
        ),
        _requirement(
            key="monitoring_events",
            title="MSRP monitoring events",
            status=_status(
                monitoring.get("schemaVersion") == "msrp_monitoring_events_v1"
                and service_covers_monitoring_events
                and frontend_finance_reconciliation_covered,
                degraded=monitoring_error is None and service_covers_monitoring_events,
                unavailable=monitoring_error is not None,
            ),
            runtime={
                "schemaVersion": monitoring.get("schemaVersion"),
                "eventCount": monitoring_event_count,
                "timelineEventCount": monitoring_timeline_count,
                "sourceRiskCount": monitoring_source_risk_count,
                "reviewRequiredCount": monitoring_review_required_count,
                "warningCount": len(monitoring_warnings),
                "warnings": monitoring_warnings[:5],
                "error": monitoring_error,
            },
            evidence=[
                "GET /msrp/monitoring/events",
                TEST_EVIDENCE["monitoringService"],
                TEST_EVIDENCE["frontendApi"],
            ],
            note="Groups price-history changes into country/model monitoring events with source-risk and review evidence.",
        ),
        _requirement(
            key="review_queue",
            title="Review queue for price alerts and conflicts",
            status=_status(
                smoke_covers_full_contract
                and service_covers_reconciliation
                and price_alert_review_queue_covered
                and price_alert_review_schema_ok,
                degraded=(
                    (conflict_count > 0)
                    or price_alert_review_queue_covered
                    or price_alert_review_schema_ok
                ),
            ),
            runtime={
                "reconciliationConflictGroups": conflict_count,
                "reconciliationSingleSourceGroups": single_source_count,
                "priceAlertReviewQueueSchemaVersion": (
                    price_alert_review_queue.get("schemaVersion")
                ),
                "priceAlertReviewQueuePath": _display_path(
                    PRICE_ALERT_REVIEW_QUEUE_PATH
                ),
                "priceAlertReviewCaseCount": price_alert_review_case_count,
                "priceAlertReviewEffectivenessFollowUpCount": (
                    price_alert_review_follow_up_count
                ),
                "priceAlertReviewEffectivenessLinkedCount": (
                    price_alert_review_linked_count
                ),
                "priceAlertReviewEffectivenessMissingCount": (
                    price_alert_review_missing_count
                ),
                "priceAlertReviewQueueCovered": price_alert_review_queue_covered,
            },
            evidence=[
                "POST /msrp/reconciliation/review-cases",
                _display_path(PRICE_ALERT_REVIEW_QUEUE_PATH),
                TEST_EVIDENCE["workflowSmoke"],
                TEST_EVIDENCE["workflowService"],
                TEST_EVIDENCE["priceAlertReviewQueue"],
            ],
            note=(
                "Read-only audit does not queue DB review cases; write smoke "
                "covers reconciliation cases and the local price-alert queue "
                "artifact carries weekly official-evidence and sales-effect "
                "follow-up state."
            ),
        ),
        _requirement(
            key="auto_review_scoring",
            title="Automatic MSRP observation scoring",
            status=_status(service_covers_auto_review_scoring),
            runtime={
                "schemaVersion": "msrp_auto_review_score_v1",
                "method": "deterministic_weighted_rules",
                "modelAssistance": {
                    "llmFit": "conditional",
                    "neuralNetworkFit": "not_recommended_until_labeled_corpus",
                },
            },
            evidence=[TEST_EVIDENCE["workflowService"]],
            note=(
                "Ingest adds weighted deterministic auto-review evidence to "
                "match_reason_json and only uses LLM/neural assistance as a "
                "governed recommendation."
            ),
        ),
        _requirement(
            key="sales_effectiveness",
            title="Sales effectiveness after price change",
            status=_status(
                service_covers_effectiveness_positive
                and effectiveness.get("schemaVersion") == "msrp_price_sales_effectiveness_v1",
                degraded=effectiveness_error is None,
                unavailable=effectiveness_error is not None,
            ),
            runtime={
                "schemaVersion": effectiveness.get("schemaVersion"),
                "analyzedEventCount": effectiveness_summary.get("analyzedEventCount"),
                "labelCounts": effectiveness_labels,
                "error": effectiveness_error,
            },
            evidence=[
                "GET /msrp/effectiveness",
                TEST_EVIDENCE["workflowService"],
            ],
            note="Runtime may be insufficient_data until matching JATO sales months exist; unit test covers positive branch.",
        ),
        _requirement(
            key="finance_monthly_lease_subsidy_net",
            title="Finance monthly, lease, subsidy and net price",
            status=_status(
                bool(finance_count and finance_count > 0)
                and finance_validation_covered
                and finance_extractor_context_covered,
                degraded=(
                    finance_error is None
                    and smoke_covers_full_contract
                    and finance_validation_covered
                    and finance_extractor_context_covered
                ),
                unavailable=finance_error is not None,
            ),
            runtime={
                "financeObservationCount": finance_count,
                "summary": finance.get("summary", {}),
                "semanticValidationCovered": finance_validation_covered,
                "extractorPricingContextCovered": finance_extractor_context_covered,
                "error": finance_error,
            },
            evidence=[
                "GET /msrp/finance-observations",
                TEST_EVIDENCE["workflowSmoke"],
                TEST_EVIDENCE["frontendApi"],
                TEST_EVIDENCE["scraperValidation"],
                TEST_EVIDENCE["httpJsonExtractor"],
                TEST_EVIDENCE["scraplingExtractor"],
                TEST_EVIDENCE["snapshotScript"],
            ],
            note=(
                "Supports monthly payment, lease type, subsidy amount, net "
                "price after subsidy filters, and weekly Hermes snapshot "
                "archive summaries."
            ),
        ),
        _requirement(
            key="official_config_table_pipeline",
            title="Official configuration/spec table pipeline",
            status=_status(
                config_source_status == "passed"
                and config_source_count > 0
                and config_source_sync_covered,
                degraded=(
                    config_source_status == "degraded"
                    and config_source_count > 0
                    and config_source_sync_covered
                ),
                unavailable=(
                    config_source_sync_error is not None
                    or config_source_status == "failed"
                ),
            ),
            runtime={
                "sourceSyncStatus": config_source_status,
                "sourceCount": config_source_count,
                "countryCount": config_country_count,
                "countries": config_source_summary.get("countries") or [],
                "missingRequiredCountries": config_missing_countries,
                "schemaRefs": config_source_summary.get("schemaRefs") or {},
                "warehouseTables": config_source_warehouse.get("tables") or [],
                "landingAdapter": config_source_warehouse.get("landingAdapter"),
                "landingSummary": config_source_landing.get("summary") or {},
                "error": config_source_sync_error,
            },
            evidence=[
                DEFAULT_SPEC_BATCH,
                "03_Scripts/engineering_config_source_sync.py",
                TEST_EVIDENCE["configSourceSync"],
                "06_AppPlatform/backend/app/api/routes/engineering_config.py",
            ],
            note=(
                "Maps official spec/configuration sources into the unified "
                "ScrapeJob contract and declares the Engineering Config "
                "ImportBatch/VehicleTrim/TrimFeatureValue warehouse landing path."
            ),
        ),
        _requirement(
            key="multi_source_reconciliation",
            title="Multi-source reconciliation",
            status=_status(
                bool(conflict_count > 0 or single_source_count > 0),
                degraded=reconciliation_error is None and service_covers_reconciliation,
                unavailable=reconciliation_error is not None,
            ),
            runtime={
                "statusCounts": reconciliation_status_counts,
                "totalGroups": reconciliation_summary.get("totalGroups"),
                "error": reconciliation_error,
            },
            evidence=[
                "GET /msrp/reconciliation",
                TEST_EVIDENCE["workflowService"],
                TEST_EVIDENCE["frontendApi"],
                TEST_EVIDENCE["snapshotScript"],
            ],
            note=(
                "Groups latest observations by vehicle key, flags source "
                "spread conflicts, and feeds the weekly Hermes snapshot."
            ),
        ),
        _requirement(
            key="dryrun_governance",
            title="Dryrun history and Hermes governance view",
            status=_status(
                bool(dryrun_history.get("latestRunId"))
                and bool(all_countries_latest)
                and country_progress_error is None,
                degraded=country_progress_error is None or dryrun_history_error is None,
                unavailable=country_progress_error is not None and dryrun_history_error is not None,
            ),
            runtime={
                "latestRunId": dryrun_history.get("latestRunId"),
                "activeRunId": (
                    dryrun_status.get("activeRunId")
                    or stable_coverage.get("activeRunId")
                ),
                "stableLatestRunId": (
                    dryrun_status.get("stableLatestRunId")
                    or stable_coverage.get("latestRunId")
                ),
                "runCount": len(dryrun_runs),
                "allCountryLatestCount": len(all_countries_latest),
                "stableCoverage": stable_coverage,
                "sourceRepairIssueCount": source_repair_issue_count,
                "transientRecheckCount": transient_recheck_count,
                "artifactFallbackUsed": dryrun_artifact_fallback_used,
                "artifactFallback": country_progress.get("artifactFallback"),
                "overall": country_progress.get("overall"),
                "gateStatus": dryrun_gate,
                "passPct": dryrun_pass_pct,
                "errors": [item for item in [country_progress_error, dryrun_history_error] if item],
            },
            evidence=[
                "GET /hermes/msrp-country-progress",
                "GET /hermes/msrp-dryrun-history",
            ],
            note=(
                "Provides source-repair governance, active-vs-stable country "
                "coverage, transient recheck counts, and pass-rate gate evidence."
            ),
        ),
        _requirement(
            key="pipeline_orchestration",
            title="Dryrun-gated ingest pipeline wrapper",
            status=_status(script_covers_full_pipeline),
            runtime={
                "script": "03_Scripts/run_msrp_pipeline.sh",
                "statusPipelineId": "msrp_pipeline",
                "phases": [
                    "official_config_source_sync",
                    "dryrun",
                    "gate",
                    "ingest",
                    "snapshot",
                    "readiness",
                    "unified_readiness",
                    "goal_completion_audit",
                ],
            },
            evidence=[
                "03_Scripts/run_msrp_pipeline.sh",
                TEST_EVIDENCE["pipelineWrapper"],
            ],
            note=(
                "Runs official config source sync first, then dryrun and only "
                "proceeds to ingest when the v3 dryrun gate allows it; ingest "
                "reuses auto-review/materialize and snapshot/readiness refresh "
                "from the low-concurrency runner before full-pipeline unified "
                "readiness and goal completion audits are refreshed."
            ),
        ),
        _requirement(
            key="frontend_management_views",
            title="Frontend management views and API clients",
            status=_status(frontend_finance_reconciliation_covered),
            runtime={
                "financePanel": "MsrpFinanceObservationsPanel",
                "reconciliationPanel": "MsrpReconciliationPanel",
                "dryrunDashboard": "MsrpDryrunDashboard",
            },
            evidence=[
                TEST_EVIDENCE["frontendApi"],
                "06_AppPlatform/frontend/src/pages/DataManagementPage.tsx",
            ],
            note="Frontend pages reuse existing Data Management/Hermes structure.",
        ),
    ]

    status_counts: dict[str, int] = {}
    for item in requirements:
        item_status = str(item.get("status") or "unknown")
        status_counts[item_status] = status_counts.get(item_status, 0) + 1

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": _overall_status(requirements),
        "generatedAtUtc": _utc_now_iso(),
        "apiBase": client.api_base,
        "filters": {
            "country": filters.get("country"),
            "brand": filters.get("brand"),
            "jatoModel": filters.get("jato_model"),
        },
        "summary": {
            "requirementCount": len(requirements),
            "statusCounts": status_counts,
            "runtimeCounts": {
                "sourceCount": source_count,
                "observationCount": observation_count,
                "currentPriceCount": current_price_count,
                "priceHistoryRows": history_count,
                "priceAlertCount": alert_count,
                "priceAlertReviewCaseCount": price_alert_review_case_count,
                "priceAlertReviewEffectivenessLinkedCount": (
                    price_alert_review_linked_count
                ),
                "monitoringEventCount": monitoring_event_count,
                "monitoringTimelineEventCount": monitoring_timeline_count,
                "monitoringSourceRiskCount": monitoring_source_risk_count,
                "financeObservationCount": finance_count,
                "officialConfigSourceCount": config_source_count,
                "officialConfigCountryCount": config_country_count,
                "reconciliationConflictGroups": conflict_count,
                "dryrunRunCount": len(dryrun_runs),
                "dryrunAllCountryLatestCount": len(all_countries_latest),
                "dryrunSourceRepairIssueCount": source_repair_issue_count,
                "dryrunTransientRecheckCount": transient_recheck_count,
                "writeAuthRole": write_role,
            },
        },
        "requirements": requirements,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit MSRP official price enrichment readiness without writes.",
    )
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--country", default=None)
    parser.add_argument("--brand", default=None)
    parser.add_argument("--jato-model", default=None)
    parser.add_argument("--threshold-pct", type=float, default=3.0)
    parser.add_argument("--reconciliation-threshold-pct", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--auth-token", default=None)
    parser.add_argument("--user-name", default="codex-readiness")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional Hermes reports directory for JSON/Markdown artifacts.",
    )
    parser.add_argument(
        "--write-status",
        action="store_true",
        help="Write hermes/reports/pipeline_status/msrp_readiness_audit.json.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless every requirement is passed.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = _utc_now_iso()
    client = ApiClient(
        api_base=args.api_base,
        timeout_seconds=max(1, int(args.timeout_seconds)),
        auth_token=args.auth_token,
        user_name=args.user_name,
    )
    started = time.time()
    report = build_readiness_report(
        client=client,
        filters={
            "country": args.country,
            "brand": args.brand,
            "jato_model": args.jato_model,
            "threshold_pct": args.threshold_pct,
            "reconciliation_threshold_pct": args.reconciliation_threshold_pct,
        },
    )
    report["elapsedSeconds"] = round(time.time() - started, 2)
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
    if args.strict and report["status"] != "passed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
