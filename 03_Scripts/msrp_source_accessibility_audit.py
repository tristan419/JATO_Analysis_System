#!/usr/bin/env python3
"""Probe MSRP source repair URLs and classify accessibility blockers."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any
from urllib.parse import urlparse

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKLOG_PATH = (
    REPO_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_source_repair_backlog.json"
)
DEFAULT_OUT_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
SCHEMA_VERSION = "msrp_source_accessibility_audit_v1"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _host_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.hostname or "").lower().strip(".")
    return host[4:] if host.startswith("www.") else host


def _dedupe_sources(items: list[Any]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    sources: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        source_code = str(item.get("sourceCode") or item.get("code") or "").strip()
        country = str(item.get("countryCode") or item.get("country") or "").strip().lower()
        source_url = str(item.get("sourceUrl") or item.get("finalUrl") or "").strip()
        key = (country, source_code, source_url)
        if not source_code or key in seen:
            continue
        seen.add(key)
        sources.append(item)
    return sources


def source_issues_from_backlog(
    backlog: dict[str, Any],
    *,
    include_transient: bool = False,
) -> list[dict[str, Any]]:
    """Extract per-source repair targets from a backlog artifact."""
    candidates: list[Any] = []
    candidates.extend(backlog.get("sourceIssues") or [])
    for group in backlog.get("groups") or []:
        if isinstance(group, dict):
            candidates.extend(group.get("sourceRepairIssues") or [])
    if include_transient:
        candidates.extend(backlog.get("transientSourceRegressions") or [])
        for group in backlog.get("groups") or []:
            if isinstance(group, dict):
                candidates.extend(group.get("transientRegressions") or [])
    return _dedupe_sources(candidates)


def _response_text_sample(response: Any, limit: int = 500) -> str:
    text = str(getattr(response, "text", "") or "")
    return " ".join(text.split())[:limit]


def _is_anti_bot_response(
    *,
    status_code: int | None,
    host: str,
    headers: dict[str, Any],
    text_sample: str,
) -> bool:
    header_text = " ".join(
        str(value) for value in headers.values() if value is not None
    ).lower()
    body = text_sample.lower()
    return bool(
        status_code == 403
        and (
            "akamai" in header_text
            or "access denied" in body
            or "permission to access" in body
            or "errors.edgesuite.net" in body
            or host == "tesla.com"
        )
    )


def classify_probe_result(
    *,
    url: str,
    status_code: int | None,
    final_url: str | None,
    headers: dict[str, Any] | None = None,
    text_sample: str = "",
    error: str | None = None,
    error_type: str | None = None,
) -> dict[str, Any]:
    """Classify a lightweight URL probe into a source-repair action."""
    host = _host_from_url(final_url or url)
    response_headers = headers or {}
    if not url:
        return {
            "probeStatus": "missing_source_url",
            "recommendedAction": "update_source_url",
            "retryable": False,
            "officialProxyRequired": False,
        }
    if error:
        lowered = error.lower()
        if error_type == "Timeout" or "timed out" in lowered or "timeout" in lowered:
            return {
                "probeStatus": "network_timeout",
                "recommendedAction": "retry_network_or_proxy",
                "retryable": True,
                "officialProxyRequired": False,
                "error": error,
            }
        if (
            error_type == "ConnectionError"
            or "connection" in lowered
            or "resolve" in lowered
            or "dns" in lowered
            or "nodename" in lowered
        ):
            return {
                "probeStatus": "network_unreachable",
                "recommendedAction": "retry_network_or_proxy",
                "retryable": True,
                "officialProxyRequired": False,
                "error": error,
            }
        return {
            "probeStatus": "probe_error",
            "recommendedAction": "manual_review_source_access",
            "retryable": True,
            "officialProxyRequired": False,
            "error": error,
        }
    if _is_anti_bot_response(
        status_code=status_code,
        host=host,
        headers=response_headers,
        text_sample=text_sample,
    ):
        return {
            "probeStatus": "anti_bot_blocked",
            "recommendedAction": "official_proxy_or_configurator_api",
            "retryable": False,
            "officialProxyRequired": True,
        }
    if status_code == 403:
        return {
            "probeStatus": "forbidden_403",
            "recommendedAction": "manual_review_or_proxy_required",
            "retryable": False,
            "officialProxyRequired": True,
        }
    if status_code == 404:
        return {
            "probeStatus": "source_url_not_found",
            "recommendedAction": "update_source_url",
            "retryable": False,
            "officialProxyRequired": False,
        }
    if status_code in {408, 425, 429} or (status_code and status_code >= 500):
        return {
            "probeStatus": "site_or_rate_limit_unstable",
            "recommendedAction": "retry_low_concurrency_or_proxy",
            "retryable": True,
            "officialProxyRequired": False,
        }
    if status_code and 200 <= status_code < 400:
        return {
            "probeStatus": "fetchable",
            "recommendedAction": "run_page_analyzer_or_selector_repair",
            "retryable": False,
            "officialProxyRequired": False,
        }
    return {
        "probeStatus": "unknown_http_status",
        "recommendedAction": "manual_review_source_access",
        "retryable": True,
        "officialProxyRequired": False,
    }


def _request_source(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: float,
) -> tuple[str, Any]:
    response = session.head(url, allow_redirects=True, timeout=timeout_seconds)
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code in {400, 403, 405, 406, 501}:
        response = session.get(url, allow_redirects=True, timeout=timeout_seconds)
        return "GET", response
    return "HEAD", response


def probe_source(
    source: dict[str, Any],
    *,
    session: requests.Session,
    timeout_seconds: float = 12.0,
) -> dict[str, Any]:
    source_url = str(source.get("sourceUrl") or source.get("finalUrl") or "").strip()
    started = time.perf_counter()
    method = None
    response = None
    error = None
    error_type = None
    if source_url:
        try:
            method, response = _request_source(
                session,
                source_url,
                timeout_seconds=timeout_seconds,
            )
        except requests.Timeout as exc:
            error = str(exc)
            error_type = "Timeout"
        except requests.ConnectionError as exc:
            error = str(exc)
            error_type = "ConnectionError"
        except requests.RequestException as exc:
            error = str(exc)
            error_type = type(exc).__name__
    elapsed_ms = round((time.perf_counter() - started) * 1000)

    has_response = response is not None
    status_code = int(getattr(response, "status_code", 0) or 0) if has_response else None
    final_url = str(getattr(response, "url", "") or source_url) if has_response else source_url
    headers = dict(getattr(response, "headers", {}) or {}) if has_response else {}
    text_sample = _response_text_sample(response) if has_response else ""
    classification = classify_probe_result(
        url=source_url,
        status_code=status_code,
        final_url=final_url,
        headers=headers,
        text_sample=text_sample,
        error=error,
        error_type=error_type,
    )
    return {
        "countryCode": str(source.get("countryCode") or source.get("country") or "").lower(),
        "sourceCode": source.get("sourceCode") or source.get("code"),
        "brand": source.get("brand"),
        "sourceUrl": source_url,
        "host": _host_from_url(final_url or source_url),
        "dryrunFailureReason": source.get("failureReason"),
        "dryrunRecommendedStrategy": source.get("recommendedStrategy"),
        "dryrunRecommendedAction": source.get("recommendedAction"),
        "method": method,
        "httpStatus": status_code,
        "finalUrl": final_url,
        "elapsedMs": elapsed_ms,
        "textSample": text_sample if classification["probeStatus"] != "fetchable" else "",
        **classification,
    }


def _increment(target: dict[str, int], value: Any) -> None:
    key = str(value or "unknown")
    target[key] = target.get(key, 0) + 1


def summarize_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    retryable_count = 0
    proxy_required_count = 0
    for item in items:
        _increment(status_counts, item.get("probeStatus"))
        _increment(action_counts, item.get("recommendedAction"))
        if item.get("retryable"):
            retryable_count += 1
        if item.get("officialProxyRequired"):
            proxy_required_count += 1
    return {
        "probedSourceCount": len(items),
        "probeStatusCounts": status_counts,
        "recommendedActionCounts": action_counts,
        "retryableNetworkCount": retryable_count,
        "officialProxyRequiredCount": proxy_required_count,
    }


def build_accessibility_report(
    backlog: dict[str, Any],
    *,
    session: requests.Session,
    timeout_seconds: float = 12.0,
    include_transient: bool = False,
) -> dict[str, Any]:
    sources = source_issues_from_backlog(
        backlog,
        include_transient=include_transient,
    )
    items = [
        probe_source(
            source,
            session=session,
            timeout_seconds=timeout_seconds,
        )
        for source in sources
    ]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": _utc_now_iso(),
        "backlogRunId": backlog.get("runId"),
        "includeTransient": include_transient,
        "summary": {
            "sourceRepairIssueCount": int(backlog.get("sourceRepairIssueCount") or 0),
            "transientRegressionCount": int(backlog.get("transientRegressionCount") or 0),
            **summarize_items(items),
        },
        "items": items,
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    summary = report.get("summary") or {}
    lines = [
        "# MSRP Source Accessibility Audit",
        "",
        f"Generated: {report.get('generatedAt') or '-'}",
        f"Backlog run: {report.get('backlogRunId') or '-'}",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Probed sources | {summary.get('probedSourceCount', 0)} |",
        f"| Source repair issues | {summary.get('sourceRepairIssueCount', 0)} |",
        f"| Transient regressions | {summary.get('transientRegressionCount', 0)} |",
        f"| Retryable network | {summary.get('retryableNetworkCount', 0)} |",
        f"| Official proxy required | {summary.get('officialProxyRequiredCount', 0)} |",
        "",
        "| Country | Source | HTTP | Probe status | Recommended action | URL |",
        "|---|---|---:|---|---|---|",
    ]
    for item in report.get("items") or []:
        lines.append(
            "| {country} | `{source}` | {status} | {probe} | {action} | {url} |".format(
                country=str(item.get("countryCode") or "").upper(),
                source=item.get("sourceCode") or "-",
                status=item.get("httpStatus") or "-",
                probe=item.get("probeStatus") or "-",
                action=item.get("recommendedAction") or "-",
                url=item.get("finalUrl") or item.get("sourceUrl") or "-",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    backlog_path: str | None = None,
    out_dir: str | None = None,
    *,
    timeout_seconds: float = 12.0,
    include_transient: bool = False,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    backlog = _load_json(Path(backlog_path or DEFAULT_BACKLOG_PATH))
    output_dir = Path(out_dir).resolve() if out_dir else DEFAULT_OUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    owns_session = session is None
    current_session = session or requests.Session()
    current_session.headers.update(DEFAULT_HEADERS)
    try:
        report = build_accessibility_report(
            backlog,
            session=current_session,
            timeout_seconds=timeout_seconds,
            include_transient=include_transient,
        )
    finally:
        if owns_session:
            current_session.close()
    json_path = output_dir / "msrp_source_accessibility_audit.json"
    md_path = output_dir / "msrp_source_accessibility_audit.md"
    json_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_markdown(report, md_path)
    print(f"[accessibility] JSON: {json_path}")
    print(f"[accessibility] Markdown: {md_path}")
    print(
        "[accessibility] "
        f"{report['summary']['probedSourceCount']} sources probed; "
        f"proxyRequired={report['summary']['officialProxyRequiredCount']}; "
        f"retryable={report['summary']['retryableNetworkCount']}"
    )
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe MSRP source repair URLs and classify accessibility blockers.",
    )
    parser.add_argument("--backlog", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=12.0)
    parser.add_argument(
        "--include-transient",
        action="store_true",
        help="Also probe transient regressions with last-known-good evidence.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run(
        backlog_path=args.backlog,
        out_dir=args.out_dir,
        timeout_seconds=max(1.0, args.timeout_seconds),
        include_transient=args.include_transient,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
