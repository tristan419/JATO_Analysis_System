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
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKLOG_PATH = (
    REPO_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_source_repair_backlog.json"
)
DEFAULT_OUT_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
DEFAULT_SOURCE_DRAFT_ROOT = (
    REPO_ROOT
    / "07_ScrapingToolkit"
    / "source_drafts"
    / "suv_only_country_model_top30"
)
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


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(path)


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


def _brand_from_source_code(source_code: str) -> str:
    first_token = source_code.strip().split("_", 1)[0]
    return first_token.upper() if first_token else ""


def _csv_filter(values: str | None) -> set[str]:
    if not values:
        return set()
    return {
        item.strip().lower()
        for item in values.split(",")
        if item.strip()
    }


def _iter_source_draft_files(
    source_draft_root: Path,
    *,
    countries: set[str],
) -> list[Path]:
    files: list[Path] = []
    for path in sorted(source_draft_root.rglob("*.yaml")):
        try:
            relative = path.relative_to(source_draft_root)
        except ValueError:
            continue
        if path.name.startswith("_") or any(part.startswith("_") for part in relative.parts):
            continue
        country = relative.parts[0].lower() if relative.parts else ""
        if countries and country not in countries:
            continue
        files.append(path)
    return files


def source_issues_from_source_drafts(
    source_draft_root: Path,
    *,
    countries: set[str] | None = None,
    brands: set[str] | None = None,
    source_codes: set[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Load current source draft URLs as accessibility probe targets."""
    country_filter = countries or set()
    brand_filter = {brand.upper() for brand in (brands or set())}
    code_filter = source_codes or set()
    sources: list[dict[str, Any]] = []
    for path in _iter_source_draft_files(source_draft_root, countries=country_filter):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError):
            continue
        if not isinstance(data, dict):
            continue
        profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
        source_code = str(data.get("source_code") or path.stem).strip()
        country_code = path.relative_to(source_draft_root).parts[0].lower()
        brand = str(data.get("brand") or _brand_from_source_code(source_code)).strip().upper()
        if brand_filter and brand not in brand_filter:
            continue
        if code_filter and source_code not in code_filter:
            continue
        source_url = str(data.get("source_url") or profile.get("url") or "").strip()
        source_item = {
            "countryCode": country_code,
            "sourceCode": source_code,
            "sourceUrl": source_url,
            "brand": brand,
            "failureReason": "source_draft_url_probe",
            "recommendedStrategy": "probe_current_source_url",
            "recommendedAction": "verify_current_source_draft_url",
            "sourceDraftPath": _display_path(path),
        }
        if str(data.get("extractor_type") or "").strip() == "pdf_text":
            source_item["extractorType"] = "pdf_text"
        if bool(profile.get("browser_download_fallback")):
            source_item["browserDownloadFallback"] = True
        sources.append(source_item)
        if limit and len(sources) >= limit:
            break
    return sources


def _legacy_sample_url_lookup(group: dict[str, Any]) -> dict[str, tuple[str, str]]:
    lookup: dict[str, tuple[str, str]] = {}
    for host_group in group.get("topSourceHosts") or []:
        if not isinstance(host_group, dict):
            continue
        host = str(host_group.get("host") or "").strip().lower()
        sources = [
            str(source).strip()
            for source in host_group.get("sampleSources") or []
            if str(source).strip()
        ]
        urls = [
            str(url).strip()
            for url in host_group.get("sampleUrls") or []
            if str(url).strip()
        ]
        for index, source_code in enumerate(sources):
            lookup.setdefault(
                source_code,
                (urls[index] if index < len(urls) else "", host),
            )
    return lookup


def _legacy_group_source_items(
    group: dict[str, Any],
    *,
    include_transient: bool,
) -> list[dict[str, Any]]:
    """Recover source samples from older backlog artifacts.

    Earlier v1 backlog artifacts stored source repair counts plus sample
    source names, but not the full sourceIssues arrays. Use the transient
    samples to avoid treating known last-good regressions as source repairs.
    When transient samples are truncated, cap the remaining repair candidates
    by sourceRepairIssueCount.
    """
    sample_sources = [
        str(source).strip()
        for source in group.get("sampleSources") or []
        if str(source).strip()
    ]
    if not sample_sources:
        return []

    transient_samples = [
        item
        for item in (
            (group.get("transientRegressions") or [])
            + (group.get("sampleTransientRegressions") or [])
        )
        if isinstance(item, dict)
    ]
    transient_codes = {
        str(item.get("sourceCode") or item.get("code") or "").strip()
        for item in transient_samples
        if str(item.get("sourceCode") or item.get("code") or "").strip()
    }
    source_repair_count = int(group.get("sourceRepairIssueCount") or 0)
    repair_sources = [
        source_code
        for source_code in sample_sources
        if source_code not in transient_codes
    ][:source_repair_count]
    url_lookup = _legacy_sample_url_lookup(group)
    countries = [
        str(country).strip().lower()
        for country in group.get("affectedCountries") or []
        if str(country).strip()
    ]
    default_country = countries[0] if len(countries) == 1 else ""

    items: list[dict[str, Any]] = []
    for source_code in repair_sources:
        source_url, host = url_lookup.get(source_code, ("", ""))
        items.append({
            "countryCode": default_country,
            "sourceCode": source_code,
            "sourceUrl": source_url,
            "host": host or _host_from_url(source_url),
            "brand": _brand_from_source_code(source_code),
            "failureReason": group.get("failureReason"),
            "recommendedStrategy": group.get("recommendedStrategy"),
            "recommendedAction": group.get("recommendedAction"),
        })
    if include_transient:
        for item in transient_samples:
            source_code = str(item.get("sourceCode") or item.get("code") or "").strip()
            if not source_code:
                continue
            source_url, host = url_lookup.get(source_code, ("", ""))
            payload = dict(item)
            payload.setdefault("countryCode", default_country)
            payload.setdefault("sourceUrl", source_url)
            payload.setdefault("host", host or _host_from_url(source_url))
            payload.setdefault("brand", _brand_from_source_code(source_code))
            items.append(payload)
    return items


def source_issues_from_backlog(
    backlog: dict[str, Any],
    *,
    include_transient: bool = False,
) -> list[dict[str, Any]]:
    """Extract per-source repair targets from a backlog artifact."""
    candidates: list[Any] = []
    candidates.extend(backlog.get("sourceIssues") or [])
    candidates.extend(backlog.get("externalAccessIssues") or [])
    for group in backlog.get("groups") or []:
        if isinstance(group, dict):
            candidates.extend(group.get("sourceRepairIssues") or [])
            candidates.extend(group.get("externalAccessIssues") or [])
    if include_transient:
        candidates.extend(backlog.get("transientSourceRegressions") or [])
        for group in backlog.get("groups") or []:
            if isinstance(group, dict):
                candidates.extend(group.get("transientRegressions") or [])
    sources = _dedupe_sources(candidates)
    if sources:
        return sources

    legacy_candidates: list[dict[str, Any]] = []
    for group in backlog.get("groups") or []:
        if isinstance(group, dict):
            legacy_candidates.extend(
                _legacy_group_source_items(
                    group,
                    include_transient=include_transient,
                )
            )
    return _dedupe_sources(legacy_candidates)



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
    anti_bot_body = (
        "access denied" in body
        or "permission to access" in body
        or "errors.edgesuite.net" in body
        or ("request denied" in body and "event id" in body)
        or ("just a moment" in body and "challenge-platform" in body)
        or "enable javascript and cookies to continue" in body
        or "__cf_chl" in body
        or "cf_chl" in body
    )
    return bool(
        (
            anti_bot_body
            and (status_code in {200, 403, 406, 429, 503} or status_code is None)
        )
        or (
            status_code == 403
            and (
                "akamai" in header_text
                or host == "tesla.com"
            )
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
            "ssleoferror" in lowered
            or "[ssl:" in lowered
            or "sslerror" in lowered
            or "tls" in lowered
        ):
            return {
                "probeStatus": "tls_handshake_failed",
                "recommendedAction": "try_official_alternative_url_or_proxy",
                "retryable": True,
                "officialProxyRequired": False,
                "error": error,
            }
        if (
            "nameresolutionerror" in lowered
            or "failed to resolve" in lowered
            or "nodename" in lowered
            or "dns" in lowered
        ):
            return {
                "probeStatus": "dns_unresolved",
                "recommendedAction": "check_dns_or_source_domain",
                "retryable": True,
                "officialProxyRequired": False,
                "error": error,
            }
        if (
            error_type == "ConnectionError"
            or "connection" in lowered
            or "resolve" in lowered
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
    if status_code in {400, 403, 405, 406, 501, 503}:
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
    if (
        source.get("extractorType") == "pdf_text"
        and source.get("browserDownloadFallback")
        and classification.get("officialProxyRequired")
    ):
        classification = {
            **classification,
            "probeStatus": "browser_fallback_required",
            "recommendedAction": "run_pdf_text_browser_fallback",
            "retryable": False,
            "officialProxyRequired": False,
        }
    return {
        "countryCode": str(source.get("countryCode") or source.get("country") or "").lower(),
        "sourceCode": source.get("sourceCode") or source.get("code"),
        "brand": source.get("brand"),
        "sourceUrl": source_url,
        "host": _host_from_url(final_url or source_url),
        "dryrunFailureReason": source.get("failureReason"),
        "dryrunRecommendedStrategy": source.get("recommendedStrategy"),
        "dryrunRecommendedAction": source.get("recommendedAction"),
        "sourceDraftPath": source.get("sourceDraftPath"),
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
    tls_failed_count = 0
    dns_unresolved_count = 0
    for item in items:
        probe_status = item.get("probeStatus")
        _increment(status_counts, probe_status)
        _increment(action_counts, item.get("recommendedAction"))
        if item.get("retryable"):
            retryable_count += 1
        if item.get("officialProxyRequired"):
            proxy_required_count += 1
        if probe_status == "tls_handshake_failed":
            tls_failed_count += 1
        if probe_status == "dns_unresolved":
            dns_unresolved_count += 1
    return {
        "probedSourceCount": len(items),
        "probeStatusCounts": status_counts,
        "recommendedActionCounts": action_counts,
        "retryableNetworkCount": retryable_count,
        "officialProxyRequiredCount": proxy_required_count,
        "tlsHandshakeFailedCount": tls_failed_count,
        "dnsUnresolvedCount": dns_unresolved_count,
    }


def _build_accessibility_report_from_sources(
    sources: list[dict[str, Any]],
    *,
    session: requests.Session,
    timeout_seconds: float = 12.0,
    source_mode: str,
    backlog: dict[str, Any] | None = None,
    include_transient: bool = False,
    source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
        "sourceMode": source_mode,
        "backlogRunId": (backlog or {}).get("runId"),
        "includeTransient": include_transient,
        "sourceContext": source_context or {},
        "summary": {
            "sourceRepairIssueCount": int((backlog or {}).get("sourceRepairIssueCount") or 0),
            "transientRegressionCount": int((backlog or {}).get("transientRegressionCount") or 0),
            "sourceDraftSourceCount": len(sources) if source_mode == "source_drafts" else 0,
            **summarize_items(items),
        },
        "items": items,
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
    return _build_accessibility_report_from_sources(
        sources,
        session=session,
        timeout_seconds=timeout_seconds,
        source_mode="backlog",
        backlog=backlog,
        include_transient=include_transient,
    )


def build_source_draft_accessibility_report(
    *,
    source_draft_root: Path,
    countries: set[str] | None,
    brands: set[str] | None,
    source_codes: set[str] | None,
    limit: int | None,
    session: requests.Session,
    timeout_seconds: float = 12.0,
) -> dict[str, Any]:
    sources = source_issues_from_source_drafts(
        source_draft_root,
        countries=countries,
        brands=brands,
        source_codes=source_codes,
        limit=limit,
    )
    return _build_accessibility_report_from_sources(
        sources,
        session=session,
        timeout_seconds=timeout_seconds,
        source_mode="source_drafts",
        source_context={
            "sourceDraftRoot": _display_path(source_draft_root),
            "countries": sorted(countries or []),
            "brands": sorted(brands or []),
            "sourceCodes": sorted(source_codes or []),
            "limit": limit,
        },
    )


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    summary = report.get("summary") or {}
    context = report.get("sourceContext") if isinstance(report.get("sourceContext"), dict) else {}
    lines = [
        "# MSRP Source Accessibility Audit",
        "",
        f"Generated: {report.get('generatedAt') or '-'}",
        f"Source mode: {report.get('sourceMode') or 'backlog'}",
        f"Backlog run: {report.get('backlogRunId') or '-'}",
        f"Source draft root: {context.get('sourceDraftRoot') or '-'}",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Probed sources | {summary.get('probedSourceCount', 0)} |",
        f"| Source repair issues | {summary.get('sourceRepairIssueCount', 0)} |",
        f"| Transient regressions | {summary.get('transientRegressionCount', 0)} |",
        f"| Source draft targets | {summary.get('sourceDraftSourceCount', 0)} |",
        f"| Retryable network | {summary.get('retryableNetworkCount', 0)} |",
        f"| Official proxy required | {summary.get('officialProxyRequiredCount', 0)} |",
        f"| TLS handshake failed | {summary.get('tlsHandshakeFailedCount', 0)} |",
        f"| DNS unresolved | {summary.get('dnsUnresolvedCount', 0)} |",
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
    source_draft_root: str | None = None,
    countries: str | None = None,
    brands: str | None = None,
    source_codes: list[str] | None = None,
    limit: int | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    output_dir = Path(out_dir).resolve() if out_dir else DEFAULT_OUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    owns_session = session is None
    current_session = session or requests.Session()
    current_session.headers.update(DEFAULT_HEADERS)
    try:
        if source_draft_root:
            code_filter = {
                code.strip()
                for code in (source_codes or [])
                if code.strip()
            }
            report = build_source_draft_accessibility_report(
                source_draft_root=Path(source_draft_root).resolve(),
                countries=_csv_filter(countries),
                brands=_csv_filter(brands),
                source_codes=code_filter,
                limit=limit,
                session=current_session,
                timeout_seconds=timeout_seconds,
            )
        else:
            backlog = _load_json(Path(backlog_path or DEFAULT_BACKLOG_PATH))
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
        "--source-draft-root",
        default=None,
        help=(
            "Probe current source draft YAML URLs instead of backlog samples. "
            f"Use {DEFAULT_SOURCE_DRAFT_ROOT} for SUV Top30 drafts."
        ),
    )
    parser.add_argument(
        "--countries",
        default=None,
        help="Comma-separated country codes to include when probing source drafts.",
    )
    parser.add_argument(
        "--brands",
        default=None,
        help="Comma-separated brand names to include when probing source drafts.",
    )
    parser.add_argument(
        "--source-code",
        action="append",
        default=[],
        help="Specific source_code to probe from source drafts; repeatable.",
    )
    parser.add_argument("--limit", type=int, default=None)
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
        source_draft_root=args.source_draft_root,
        countries=args.countries,
        brands=args.brands,
        source_codes=args.source_code,
        limit=args.limit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
