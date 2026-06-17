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
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
if str(HERMES_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_SCRIPT_DIR))

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
    "snapshotScript": "03_Scripts/tests/test_hermes_msrp_current_price_snapshot.py",
    "frontendApi": "06_AppPlatform/frontend/src/tests/unit/dataManagementApi.test.ts",
    "pipelineWrapper": "03_Scripts/tests/test_run_msrp_pipeline.py",
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
    country_progress, country_progress_error = _safe_get(
        client,
        "/hermes/msrp-country-progress",
    )
    dryrun_history, dryrun_history_error = _safe_get(
        client,
        "/hermes/msrp-dryrun-history",
    )

    source_count = _count_from_payload(sources, "total", "rows")
    observation_count = _count_from_payload(observations, "total", "rows")
    current_price_count = _count_from_payload(current_prices, "total", "rows")
    history_count = _count_from_payload(history, "rows", "total")
    alert_count = _count_from_payload(alerts, "total", "rows")
    snapshot_current_count = _summary_count(snapshot, "currentPriceCount")
    snapshot_alert_count = _summary_count(snapshot, "priceAlertCount")
    finance_count = _count_from_payload(finance, "total", "rows")
    reconciliation_summary = _nested_dict(reconciliation, "summary")
    reconciliation_status_counts = _nested_dict(reconciliation_summary, "statusCounts")
    conflict_count = int(reconciliation_status_counts.get("conflict") or 0)
    single_source_count = int(reconciliation_status_counts.get("single_source") or 0)
    effectiveness_summary = _nested_dict(effectiveness, "summary")
    effectiveness_labels = _nested_dict(effectiveness_summary, "labelCounts")
    dryrun_status = _nested_dict(country_progress, "status")
    dryrun_runs = dryrun_history.get("runs") if isinstance(dryrun_history.get("runs"), list) else []
    dryrun_pass_pct = dryrun_status.get("overallPassPct")
    dryrun_gate = dryrun_status.get("gateStatus")
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
    script_covers_full_pipeline = _test_file_has(
        "pipelineWrapper",
        "test_pipeline_runs_ingest_when_dryrun_gate_is_allowed",
        "test_pipeline_skips_ingest_when_dryrun_gate_blocks",
        "msrp_pipeline",
    )
    snapshot_script_covered = _test_file_has(
        "snapshotScript",
        "msrp_current_price_snapshot_v1",
        "test_run_writes_degraded_status_for_high_priority_alert",
    )
    frontend_finance_reconciliation_covered = _test_file_has(
        "frontendApi",
        "listMsrpFinanceObservations",
        "listMsrpReconciliation",
        "queueMsrpReconciliationReviewCases",
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
                and bool(snapshot.get("snapshotWeek")),
                degraded=snapshot_error is None,
                unavailable=snapshot_error is not None,
            ),
            runtime={
                "schemaVersion": snapshot.get("schemaVersion"),
                "snapshotWeek": snapshot.get("snapshotWeek"),
                "currentPriceCount": snapshot_current_count,
                "priceAlertCount": snapshot_alert_count,
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
            key="review_queue",
            title="Review queue for low-confidence/conflict cases",
            status=_status(
                smoke_covers_full_contract and service_covers_reconciliation,
                degraded=conflict_count > 0,
            ),
            runtime={
                "reconciliationConflictGroups": conflict_count,
                "reconciliationSingleSourceGroups": single_source_count,
            },
            evidence=[
                "POST /msrp/reconciliation/review-cases",
                TEST_EVIDENCE["workflowSmoke"],
                TEST_EVIDENCE["workflowService"],
            ],
            note="Read-only audit does not queue review cases; write smoke covers the queue path.",
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
                bool(finance_count and finance_count > 0),
                degraded=finance_error is None and smoke_covers_full_contract,
                unavailable=finance_error is not None,
            ),
            runtime={
                "financeObservationCount": finance_count,
                "summary": finance.get("summary", {}),
                "error": finance_error,
            },
            evidence=[
                "GET /msrp/finance-observations",
                TEST_EVIDENCE["workflowSmoke"],
                TEST_EVIDENCE["frontendApi"],
            ],
            note="Supports monthly payment, lease type, subsidy amount, and net price after subsidy filters.",
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
            ],
            note="Groups latest observations by vehicle key and flags source spread conflicts.",
        ),
        _requirement(
            key="dryrun_governance",
            title="Dryrun history and Hermes governance view",
            status=_status(
                bool(dryrun_history.get("latestRunId")) and country_progress_error is None,
                degraded=country_progress_error is None or dryrun_history_error is None,
                unavailable=country_progress_error is not None and dryrun_history_error is not None,
            ),
            runtime={
                "latestRunId": dryrun_history.get("latestRunId"),
                "runCount": len(dryrun_runs),
                "overall": country_progress.get("overall"),
                "gateStatus": dryrun_gate,
                "passPct": dryrun_pass_pct,
                "errors": [item for item in [country_progress_error, dryrun_history_error] if item],
            },
            evidence=[
                "GET /hermes/msrp-country-progress",
                "GET /hermes/msrp-dryrun-history",
            ],
            note="Provides source-repair governance and pass-rate gate evidence.",
        ),
        _requirement(
            key="pipeline_orchestration",
            title="Dryrun-gated ingest pipeline wrapper",
            status=_status(script_covers_full_pipeline),
            runtime={
                "script": "03_Scripts/run_msrp_pipeline.sh",
                "statusPipelineId": "msrp_pipeline",
                "phases": [
                    "dryrun",
                    "gate",
                    "ingest",
                    "snapshot",
                    "readiness",
                ],
            },
            evidence=[
                "03_Scripts/run_msrp_pipeline.sh",
                TEST_EVIDENCE["pipelineWrapper"],
            ],
            note=(
                "Runs dryrun and only proceeds to ingest when the v3 dryrun "
                "gate allows it; ingest reuses auto-review/materialize and "
                "snapshot/readiness refresh from the low-concurrency runner."
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
                "financeObservationCount": finance_count,
                "reconciliationConflictGroups": conflict_count,
                "dryrunRunCount": len(dryrun_runs),
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
