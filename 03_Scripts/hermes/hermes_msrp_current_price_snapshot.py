#!/usr/bin/env python3
"""Generate Hermes MSRP current price snapshot artifacts.

The backend owns the current price, price history, and alert semantics. This
script calls that read API and materializes the weekly snapshot into Hermes
JSON/Markdown artifacts so the result can be archived by deploy jobs.
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
    effectiveness_summary = (
        effectiveness.get("summary")
        if isinstance(effectiveness, dict)
        else {}
    ) or summary.get("effectivenessSummary") or {}
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
            "| Country | Brand | Model | Trim | Direction | Severity | Delta % | Action |",
            "|---|---|---|---|---|---|---:|---|",
        ])
        for item in alerts[:50]:
            lines.append(
                "| "
                + " | ".join([
                    _markdown_cell(item.get("country")),
                    _markdown_cell(item.get("brand")),
                    _markdown_cell(item.get("jatoModel")),
                    _markdown_cell(item.get("jatoTrim")),
                    _markdown_cell(item.get("direction")),
                    _markdown_cell(item.get("severity")),
                    _number(item.get("deltaPct")),
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
            "| Country | Brand | Model | Event month | Price direction | Sales delta % | Label |",
            "|---|---|---|---|---|---:|---|",
        ])
        for item in effectiveness_items[:50]:
            lines.append(
                "| "
                + " | ".join([
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
    return "\n".join(lines) + "\n"


def _write_failure_status(error: Exception, started_at: str) -> None:
    write_pipeline_status(
        pipeline_id="msrp_current_price_snapshot",
        status="failed",
        started_at=started_at,
        finished_at=_utc_now(),
        exit_code=1,
        failed_count=1,
        source="hermes_msrp_current_price_snapshot",
        message=str(error)[:500],
        extra={"errorType": type(error).__name__},
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
    baseline_window_months: int = 3,
    post_window_months: int = 3,
    post_lag_months: int = 1,
    min_months: int = 1,
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
    effectiveness_warning_count = 0
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
    alert_summary = summary.get("priceAlertSummary") or {}
    current_count = int(summary.get("currentPriceCount") or 0)
    high_priority = int(alert_summary.get("highPriorityAlertCount") or 0)
    threshold_alerts = int(alert_summary.get("thresholdAlertCount") or 0)
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
    if effectiveness_warning_count:
        status = "degraded"
    warning_count = (
        threshold_alerts
        + (1 if current_count == 0 else 0)
        + effectiveness_warning_count
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
            f"{effectiveness_summary.get('analyzedEventCount', 0)}."
        ),
        extra={
            "snapshotWeek": snapshot.get("snapshotWeek"),
            "priceAlertSummary": alert_summary,
            "effectivenessSummary": effectiveness_summary,
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
    parser.add_argument("--baseline-window-months", type=int, default=3)
    parser.add_argument("--post-window-months", type=int, default=3)
    parser.add_argument("--post-lag-months", type=int, default=1)
    parser.add_argument("--min-months", type=int, default=1)
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
            baseline_window_months=max(1, args.baseline_window_months),
            post_window_months=max(1, args.post_window_months),
            post_lag_months=max(0, args.post_lag_months),
            min_months=max(1, args.min_months),
        )
    except Exception as exc:
        _write_failure_status(exc, started_at)
        print(f"[msrp-current-snapshot] failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(f"[msrp-current-snapshot] status={result['pipelineStatus']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
