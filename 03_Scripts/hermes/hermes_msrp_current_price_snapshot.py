#!/usr/bin/env python3
"""Generate Hermes MSRP current price snapshot artifacts.

The backend owns the current price, price history, alert, reconciliation, and
finance semantics. This script calls those read APIs and materializes the
weekly snapshot into Hermes JSON/Markdown artifacts so the result can be
archived by deploy jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - supports direct module execution.
    sys.path.append(str(Path(__file__).resolve().parent))
    from pipeline_status_writer import write_pipeline_status


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_API_BASE = "http://127.0.0.1:8000/v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _snapshot_week(timestamp: str | None = None) -> str:
    try:
        raw = str(timestamp or _utc_now()).replace("Z", "+00:00")
        current = datetime.fromisoformat(raw)
    except ValueError:
        current = datetime.now(timezone.utc)
    year, week, _ = current.isocalendar()
    return f"{year}-W{week:02d}"


def _failure_snapshot(
    error: Exception,
    *,
    started_at: str,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float,
) -> dict[str, Any]:
    warning = f"snapshot_fetch_failed:{type(error).__name__}"
    return {
        "schemaVersion": "msrp_current_price_snapshot_v1",
        "generatedAtUtc": _utc_now(),
        "snapshotWeek": _snapshot_week(started_at),
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
        },
        "summary": {
            "currentPriceCount": 0,
            "returnedCurrentPriceCount": 0,
            "priceAlertCount": 0,
            "returnedPriceAlertCount": 0,
            "priceAlertThresholdPct": threshold_pct,
            "priceAlertSummary": {
                "priceChangeEventCount": 0,
                "thresholdAlertCount": 0,
                "highPriorityAlertCount": 0,
                "directionCounts": {},
                "severityCounts": {},
            },
            "effectivenessSummary": {
                "priceEventCount": 0,
                "analyzedEventCount": 0,
                "labelCounts": {},
                "limit": limit,
            },
            "reconciliationSummary": {
                "observationRows": 0,
                "reconciliationGroupCount": 0,
                "statusCounts": {},
                "limit": limit,
            },
            "financeSummary": {
                "monthlyPaymentCount": 0,
                "netPriceAfterSubsidyCount": 0,
                "subsidyObservationCount": 0,
            },
            "limit": limit,
        },
        "currentPrices": [],
        "priceAlerts": [],
        "priceSalesEffectiveness": {
            "schemaVersion": "msrp_price_sales_effectiveness_v1",
            "summary": {
                "priceEventCount": 0,
                "analyzedEventCount": 0,
                "labelCounts": {},
                "limit": limit,
            },
            "items": [],
            "warnings": [warning],
        },
        "multiSourceReconciliation": {
            "schemaVersion": "msrp_multi_source_reconciliation_v1",
            "summary": {
                "observationRows": 0,
                "reconciliationGroupCount": 0,
                "statusCounts": {},
                "limit": limit,
            },
            "items": [],
            "warnings": [warning],
        },
        "financeObservations": {
            "rows": 0,
            "total": 0,
            "limit": limit,
            "offset": 0,
            "summary": {
                "monthlyPaymentCount": 0,
                "netPriceAfterSubsidyCount": 0,
                "subsidyObservationCount": 0,
            },
            "items": [],
            "warnings": [warning],
        },
        "warnings": [warning],
        "error": {
            "type": type(error).__name__,
            "message": str(error)[:500],
        },
    }


def _fetch_snapshot(
    *,
    api_base: str,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    query = {
        "limit": str(limit),
        "threshold_pct": str(threshold_pct),
    }
    if country:
        query["country"] = country
    if brand:
        query["brand"] = brand
    if jato_model:
        query["jato_model"] = jato_model
    url = (
        api_base.rstrip("/")
        + "/msrp/current-prices/snapshot?"
        + urllib.parse.urlencode(query)
    )
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_effectiveness(
    *,
    api_base: str,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float,
    baseline_window_months: int,
    post_window_months: int,
    post_lag_months: int,
    min_months: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    query = {
        "limit": str(limit),
        "threshold_pct": str(threshold_pct),
        "baseline_window_months": str(baseline_window_months),
        "post_window_months": str(post_window_months),
        "post_lag_months": str(post_lag_months),
        "min_months": str(min_months),
    }
    if country:
        query["country"] = country
    if brand:
        query["brand"] = brand
    if jato_model:
        query["jato_model"] = jato_model
    url = (
        api_base.rstrip("/")
        + "/msrp/effectiveness?"
        + urllib.parse.urlencode(query)
    )
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_reconciliation(
    *,
    api_base: str,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    query = {
        "limit": str(limit),
        "threshold_pct": str(threshold_pct),
    }
    if country:
        query["country"] = country
    if brand:
        query["brand"] = brand
    if jato_model:
        query["jato_model"] = jato_model
    url = (
        api_base.rstrip("/")
        + "/msrp/reconciliation?"
        + urllib.parse.urlencode(query)
    )
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_finance_observations(
    *,
    api_base: str,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    query = {
        "limit": str(limit),
        "offset": "0",
    }
    if country:
        query["country"] = country
    if brand:
        query["brand"] = brand
    if jato_model:
        query["jato_model"] = jato_model
    url = (
        api_base.rstrip("/")
        + "/msrp/finance-observations?"
        + urllib.parse.urlencode(query)
    )
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_token(value: str | None, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    safe = "".join(ch if ch.isalnum() else "-" for ch in text)
    return "-".join(part for part in safe.split("-") if part) or fallback


def _history_suffix(snapshot: dict[str, Any]) -> str:
    week = _safe_token(str(snapshot.get("snapshotWeek") or ""), "unknown-week")
    generated = str(snapshot.get("generatedAtUtc") or _utc_now())
    stamp = (
        generated.replace(":", "")
        .replace("-", "")
        .replace("+", "z")
        .replace(".", "-")
    )
    stamp = _safe_token(stamp, "unknown-time")
    return f"{week}_{stamp}"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _write_outputs(snapshot: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_json = out_dir / "msrp_current_price_snapshot.json"
    latest_md = out_dir / "msrp_current_price_snapshot.md"
    suffix = _history_suffix(snapshot)
    hist_json = out_dir / f"msrp_current_price_snapshot_{suffix}.json"
    hist_md = out_dir / f"msrp_current_price_snapshot_{suffix}.md"

    json_text = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"
    md_text = _render_markdown(snapshot)
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


def _number(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get("summary") or {}
    alert_summary = summary.get("priceAlertSummary") or {}
    effectiveness = snapshot.get("priceSalesEffectiveness") or {}
    reconciliation = snapshot.get("multiSourceReconciliation") or {}
    finance = snapshot.get("financeObservations") or {}
    effectiveness_summary = (
        effectiveness.get("summary")
        if isinstance(effectiveness, dict)
        else {}
    ) or summary.get("effectivenessSummary") or {}
    reconciliation_summary = (
        reconciliation.get("summary")
        if isinstance(reconciliation, dict)
        else {}
    ) or summary.get("reconciliationSummary") or {}
    finance_summary = (
        finance.get("summary")
        if isinstance(finance, dict)
        else {}
    ) or summary.get("financeSummary") or {}
    warnings = [
        str(item)
        for item in (snapshot.get("warnings") or [])
        if str(item).strip()
    ]
    lines: list[str] = [
        "# MSRP Current Price Snapshot",
        "",
        f"**Generated:** {snapshot.get('generatedAtUtc', '-')}",
        f"**Week:** {snapshot.get('snapshotWeek', '-')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Current prices | {_number(summary.get('currentPriceCount'))} |",
        f"| Price change events | {_number(alert_summary.get('priceChangeEventCount'))} |",
        f"| Threshold alerts | {_number(alert_summary.get('thresholdAlertCount'))} |",
        f"| High-priority alerts | {_number(alert_summary.get('highPriorityAlertCount'))} |",
        f"| Alert threshold pct | {_number(summary.get('priceAlertThresholdPct'))} |",
        f"| Effectiveness events | {_number(effectiveness_summary.get('priceEventCount'))} |",
        f"| Effectiveness analyzed | {_number(effectiveness_summary.get('analyzedEventCount'))} |",
        f"| Reconciliation groups | {_number(reconciliation_summary.get('reconciliationGroupCount'))} |",
        f"| Finance observations | {_number(finance.get('total'))} |",
        f"| Finance monthly rows | {_number(finance_summary.get('monthlyPaymentCount'))} |",
        f"| Net incentive price rows | {_number(finance_summary.get('netPriceAfterSubsidyCount'))} |",
        "",
    ]
    if warnings:
        lines.extend([
            "## Warnings",
            "",
            *[f"- {_markdown_cell(item)}" for item in warnings],
            "",
        ])
    lines.extend([
        "## Price Alerts",
        "",
    ])
    alerts = list(snapshot.get("priceAlerts") or [])
    if not alerts:
        lines.append("No price alerts in this snapshot.")
    else:
        lines.extend([
            "| Event ID | Country | Brand | Model | Trim | Direction | Severity | Delta % | Evidence | Source | Action |",
            "|---|---|---|---|---|---|---|---:|---|---|---|",
        ])
        for item in alerts[:50]:
            lines.append(
                "| "
                + " | ".join([
                    _markdown_cell(item.get("alertId")),
                    _markdown_cell(item.get("country")),
                    _markdown_cell(item.get("brand")),
                    _markdown_cell(item.get("jatoModel")),
                    _markdown_cell(item.get("jatoTrim")),
                    _markdown_cell(item.get("direction")),
                    _markdown_cell(item.get("severity")),
                    _number(item.get("deltaPct")),
                    _markdown_cell(item.get("evidenceStatus")),
                    _markdown_cell(
                        item.get("sourceName")
                        or item.get("sourceType")
                        or item.get("sourceUrl")
                    ),
                    _markdown_cell(item.get("recommendedAction")),
                ])
                + " |"
            )
    lines.extend([
        "",
        "## Sales Effectiveness",
        "",
    ])
    effectiveness_items = (
        list(effectiveness.get("items") or [])
        if isinstance(effectiveness, dict)
        else []
    )
    label_counts = (
        effectiveness_summary.get("labelCounts")
        if isinstance(effectiveness_summary.get("labelCounts"), dict)
        else {}
    )
    if label_counts:
        lines.extend([
            "| Label | Count |",
            "|---|---:|",
        ])
        for label, count in sorted(label_counts.items()):
            lines.append(f"| {_markdown_cell(label)} | {_number(count)} |")
        lines.append("")
    if not effectiveness_items:
        lines.append("No price sales effectiveness rows in this snapshot.")
    else:
        lines.extend([
            "| Price event | Country | Brand | Model | Event month | Price direction | Sales delta % | Label |",
            "|---|---|---|---|---|---|---:|---|",
        ])
        for item in effectiveness_items[:50]:
            lines.append(
                "| "
                + " | ".join([
                    _markdown_cell(item.get("priceEventId")),
                    _markdown_cell(item.get("country")),
                    _markdown_cell(item.get("brand")),
                    _markdown_cell(item.get("jatoModel")),
                    _markdown_cell(item.get("priceEventMonth")),
                    _markdown_cell(item.get("priceChangeDirection")),
                    _number(item.get("salesDeltaPct")),
                    _markdown_cell(item.get("effectivenessLabel")),
                ])
                + " |"
            )
    lines.extend([
        "",
        "## Multi-source Reconciliation",
        "",
    ])
    status_counts = (
        reconciliation_summary.get("statusCounts")
        if isinstance(reconciliation_summary.get("statusCounts"), dict)
        else {}
    )
    if status_counts:
        lines.extend([
            "| Status | Count |",
            "|---|---:|",
        ])
        for status, count in sorted(status_counts.items()):
            lines.append(f"| {_markdown_cell(status)} | {_number(count)} |")
        lines.append("")
    reconciliation_items = (
        list(reconciliation.get("items") or [])
        if isinstance(reconciliation, dict)
        else []
    )
    if not reconciliation_items:
        lines.append("No multi-source reconciliation rows in this snapshot.")
    else:
        lines.extend([
            "| Country | Brand | Model | Trim | Status | Sources | Spread % | Action |",
            "|---|---|---|---|---|---:|---:|---|",
        ])
        for item in reconciliation_items[:50]:
            lines.append(
                "| "
                + " | ".join([
                    _markdown_cell(item.get("country")),
                    _markdown_cell(item.get("brand")),
                    _markdown_cell(item.get("jatoModel")),
                    _markdown_cell(item.get("jatoTrim")),
                    _markdown_cell(item.get("status")),
                    _number(item.get("sourceCount")),
                    _number(item.get("spreadPct")),
                    _markdown_cell(item.get("recommendedAction")),
                ])
                + " |"
            )
    lines.extend([
        "",
        "## Finance And Net Incentives",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Finance observations | {_number(finance.get('total'))} |",
        f"| Returned rows | {_number(finance.get('rows'))} |",
        f"| Monthly payment rows | {_number(finance_summary.get('monthlyPaymentCount'))} |",
        f"| Subsidy rows | {_number(finance_summary.get('subsidyObservationCount'))} |",
        f"| Net price after subsidy rows | {_number(finance_summary.get('netPriceAfterSubsidyCount'))} |",
        f"| Monthly EUR min | {_number(finance_summary.get('monthlyPaymentEurMin'))} |",
        f"| Monthly EUR max | {_number(finance_summary.get('monthlyPaymentEurMax'))} |",
        f"| Net price EUR min | {_number(finance_summary.get('netPriceAfterSubsidyEurMin'))} |",
        f"| Net price EUR max | {_number(finance_summary.get('netPriceAfterSubsidyEurMax'))} |",
        "",
    ])
    finance_items = (
        list(finance.get("items") or [])
        if isinstance(finance, dict)
        else []
    )
    if not finance_items:
        lines.append("No finance observation rows in this snapshot.")
    else:
        lines.extend([
            "| Country | Brand | Model | Semantics | Finance type | Monthly EUR | Net price EUR |",
            "|---|---|---|---|---|---:|---:|",
        ])
        for item in finance_items[:50]:
            lines.append(
                "| "
                + " | ".join([
                    _markdown_cell(item.get("country")),
                    _markdown_cell(item.get("brand")),
                    _markdown_cell(item.get("jatoModel")),
                    _markdown_cell(item.get("priceSemantics")),
                    _markdown_cell(item.get("financeType")),
                    _number(item.get("monthlyPaymentEur")),
                    _number(item.get("netPriceAfterSubsidyEur")),
                ])
                + " |"
            )
    return "\n".join(lines) + "\n"


def _write_failure_status(
    error: Exception,
    started_at: str,
    artifact_refs: dict[str, str] | None = None,
) -> None:
    extra: dict[str, Any] = {"errorType": type(error).__name__}
    if artifact_refs:
        extra["artifactRefsByName"] = artifact_refs
    write_pipeline_status(
        pipeline_id="msrp_current_price_snapshot",
        status="failed",
        started_at=started_at,
        finished_at=_utc_now(),
        exit_code=1,
        failed_count=1,
        artifact_refs=list((artifact_refs or {}).values()),
        source="hermes_msrp_current_price_snapshot",
        message=str(error)[:500],
        extra=extra,
        repo_root=REPO_ROOT,
    )


def run(
    *,
    api_base: str,
    out_dir: Path,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float,
    timeout_seconds: int,
    include_effectiveness: bool = True,
    include_reconciliation: bool = True,
    include_finance: bool = True,
    baseline_window_months: int = 3,
    post_window_months: int = 3,
    post_lag_months: int = 1,
    min_months: int = 1,
    reconciliation_threshold_pct: float = 1.0,
) -> dict[str, Any]:
    started_at = _utc_now()
    snapshot = _fetch_snapshot(
        api_base=api_base,
        country=country,
        brand=brand,
        jato_model=jato_model,
        limit=limit,
        threshold_pct=threshold_pct,
        timeout_seconds=timeout_seconds,
    )
    summary = snapshot.get("summary") or {}
    alert_summary = summary.get("priceAlertSummary") or {}
    warnings = [
        str(item)
        for item in (snapshot.get("warnings") or [])
        if str(item).strip()
    ]
    effectiveness_summary: dict[str, Any] = {}
    reconciliation_summary: dict[str, Any] = {}
    finance_summary: dict[str, Any] = {}
    effectiveness_warning_count = 0
    reconciliation_warning_count = 0
    finance_warning_count = 0
    if include_effectiveness:
        try:
            effectiveness = _fetch_effectiveness(
                api_base=api_base,
                country=country,
                brand=brand,
                jato_model=jato_model,
                limit=limit,
                threshold_pct=threshold_pct,
                baseline_window_months=baseline_window_months,
                post_window_months=post_window_months,
                post_lag_months=post_lag_months,
                min_months=min_months,
                timeout_seconds=timeout_seconds,
            )
            effectiveness_summary = (
                effectiveness.get("summary")
                if isinstance(effectiveness.get("summary"), dict)
                else {}
            )
            snapshot = {
                **snapshot,
                "summary": {
                    **summary,
                    "effectivenessSummary": effectiveness_summary,
                },
                "priceSalesEffectiveness": effectiveness,
            }
        except Exception as exc:  # pragma: no cover - covered by script run paths.
            effectiveness_warning_count = 1
            warnings.append(
                f"effectiveness_unavailable:{type(exc).__name__}"
            )
    summary = snapshot.get("summary") or {}
    if include_reconciliation:
        try:
            reconciliation = _fetch_reconciliation(
                api_base=api_base,
                country=country,
                brand=brand,
                jato_model=jato_model,
                limit=limit,
                threshold_pct=reconciliation_threshold_pct,
                timeout_seconds=timeout_seconds,
            )
            reconciliation_summary = (
                reconciliation.get("summary")
                if isinstance(reconciliation.get("summary"), dict)
                else {}
            )
            snapshot = {
                **snapshot,
                "summary": {
                    **summary,
                    "reconciliationSummary": reconciliation_summary,
                },
                "multiSourceReconciliation": reconciliation,
            }
            for warning in reconciliation.get("warnings") or []:
                if str(warning).strip():
                    warnings.append(f"reconciliation:{warning}")
        except Exception as exc:  # pragma: no cover - covered by script run paths.
            reconciliation_warning_count = 1
            warnings.append(
                f"reconciliation_unavailable:{type(exc).__name__}"
            )
    summary = snapshot.get("summary") or {}
    if include_finance:
        try:
            finance = _fetch_finance_observations(
                api_base=api_base,
                country=country,
                brand=brand,
                jato_model=jato_model,
                limit=limit,
                timeout_seconds=timeout_seconds,
            )
            finance_summary = (
                finance.get("summary")
                if isinstance(finance.get("summary"), dict)
                else {}
            )
            snapshot = {
                **snapshot,
                "summary": {
                    **summary,
                    "financeSummary": finance_summary,
                },
                "financeObservations": finance,
            }
            for warning in finance.get("warnings") or []:
                if str(warning).strip():
                    warnings.append(f"finance:{warning}")
            if finance.get("warning"):
                warnings.append(f"finance:{finance['warning']}")
        except Exception as exc:  # pragma: no cover - covered by script run paths.
            finance_warning_count = 1
            warnings.append(
                f"finance_unavailable:{type(exc).__name__}"
            )
    summary = snapshot.get("summary") or {}
    alert_summary = summary.get("priceAlertSummary") or {}
    current_count = int(summary.get("currentPriceCount") or 0)
    high_priority = int(alert_summary.get("highPriorityAlertCount") or 0)
    threshold_alerts = int(alert_summary.get("thresholdAlertCount") or 0)
    status_counts = reconciliation_summary.get("statusCounts")
    conflict_count = (
        int(status_counts.get("conflict") or 0)
        if isinstance(status_counts, dict)
        else 0
    )
    if current_count == 0:
        warnings.append("no_current_prices_available")
    if warnings:
        snapshot = {
            **snapshot,
            "warnings": sorted(set(warnings)),
        }
    artifact_refs = _write_outputs(snapshot, out_dir)
    finished_at = _utc_now()
    status = "degraded" if current_count == 0 or high_priority > 0 else "success"
    if conflict_count > 0:
        status = "degraded"
    if (
        effectiveness_warning_count
        or reconciliation_warning_count
        or finance_warning_count
    ):
        status = "degraded"
    warning_count = (
        threshold_alerts
        + (1 if current_count == 0 else 0)
        + effectiveness_warning_count
        + reconciliation_warning_count
        + finance_warning_count
        + conflict_count
    )

    write_pipeline_status(
        pipeline_id="msrp_current_price_snapshot",
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=0,
        records_processed=current_count,
        warning_count=warning_count,
        artifact_refs=list(artifact_refs.values()),
        source="hermes_msrp_current_price_snapshot",
        message=(
            f"Current prices={current_count}, "
            f"thresholdAlerts={threshold_alerts}, "
            f"highPriority={high_priority}, "
            f"effectivenessAnalyzed="
            f"{effectiveness_summary.get('analyzedEventCount', 0)}, "
            f"reconciliationConflicts={conflict_count}, "
            f"financeRows={snapshot.get('financeObservations', {}).get('total', 0)}."
        ),
        extra={
            "snapshotWeek": snapshot.get("snapshotWeek"),
            "priceAlertSummary": alert_summary,
            "effectivenessSummary": effectiveness_summary,
            "reconciliationSummary": reconciliation_summary,
            "financeSummary": finance_summary,
            "warnings": warnings,
        },
        repo_root=REPO_ROOT,
    )
    return {
        "snapshot": snapshot,
        "artifacts": artifact_refs,
        "pipelineStatus": status,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate Hermes MSRP current price snapshot artifacts.",
    )
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "hermes" / "reports"))
    parser.add_argument("--country")
    parser.add_argument("--brand")
    parser.add_argument("--jato-model")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--threshold-pct", type=float, default=3.0)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument(
        "--no-effectiveness",
        action="store_true",
        help="Skip /msrp/effectiveness when only current price state is needed.",
    )
    parser.add_argument(
        "--no-reconciliation",
        action="store_true",
        help="Skip /msrp/reconciliation weekly snapshot enrichment.",
    )
    parser.add_argument(
        "--no-finance",
        action="store_true",
        help="Skip /msrp/finance-observations weekly snapshot enrichment.",
    )
    parser.add_argument("--baseline-window-months", type=int, default=3)
    parser.add_argument("--post-window-months", type=int, default=3)
    parser.add_argument("--post-lag-months", type=int, default=1)
    parser.add_argument("--min-months", type=int, default=1)
    parser.add_argument("--reconciliation-threshold-pct", type=float, default=1.0)
    args = parser.parse_args(argv)

    started_at = _utc_now()
    try:
        result = run(
            api_base=args.api_base,
            out_dir=Path(args.out_dir).expanduser().resolve(),
            country=args.country,
            brand=args.brand,
            jato_model=args.jato_model,
            limit=max(1, min(args.limit, 500)),
            threshold_pct=max(0.0, args.threshold_pct),
            timeout_seconds=max(1, args.timeout_seconds),
            include_effectiveness=not args.no_effectiveness,
            include_reconciliation=not args.no_reconciliation,
            include_finance=not args.no_finance,
            baseline_window_months=max(1, args.baseline_window_months),
            post_window_months=max(1, args.post_window_months),
            post_lag_months=max(0, args.post_lag_months),
            min_months=max(1, args.min_months),
            reconciliation_threshold_pct=max(
                0.0,
                args.reconciliation_threshold_pct,
            ),
        )
    except Exception as exc:
        artifact_refs: dict[str, str] = {}
        try:
            failure_snapshot = _failure_snapshot(
                exc,
                started_at=started_at,
                country=args.country,
                brand=args.brand,
                jato_model=args.jato_model,
                limit=max(1, min(args.limit, 500)),
                threshold_pct=max(0.0, args.threshold_pct),
            )
            artifact_refs = _write_outputs(
                failure_snapshot,
                Path(args.out_dir).expanduser().resolve(),
            )
        except Exception:
            artifact_refs = {}
        _write_failure_status(exc, started_at, artifact_refs)
        print(f"[msrp-current-snapshot] failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(f"[msrp-current-snapshot] status={result['pipelineStatus']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
