#!/usr/bin/env python3
"""Inspect official MSRP source candidates for price-bearing page signals.

This audit is intentionally pre-ingest. A page that contains price-like
official MSRP signals can become a dryrun repair candidate, but it is still
not eligible for current_prices until the normal extractor, validation, and
review gates pass.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
for path in (SCRIPT_DIR, TOOLKIT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from jato_scraper.llm.msrp_page_analyzer import (  # noqa: E402
    analyze_page_heuristics,
    build_page_evidence,
)
from msrp_source_accessibility_audit import (  # noqa: E402
    DEFAULT_HEADERS,
    _host_from_url,
    classify_probe_result,
    source_issues_from_backlog,
)


SCHEMA_VERSION = "msrp_official_price_signal_audit_v1"
CAMPAIGN_PRICE_KEYWORDS = (
    "aktionspreis",
    "campaign price",
    "promotion price",
    "promotional price",
    "promo price",
    "eintauschbonus",
    "leasingbonus",
    "versicherungsbonus",
    "bonus",
    "rabatt",
    "discount",
    "subsidy",
    "net price",
    "nettopreis",
    "cash",
)
DEFAULT_BACKLOG_PATH = (
    REPO_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_source_repair_backlog.json"
)
DEFAULT_OUT_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"


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


def _parse_candidate_url_args(values: list[str] | None) -> dict[str, list[str]]:
    candidate_map: dict[str, list[str]] = {}
    for raw_value in values or []:
        if "=" not in raw_value:
            raise ValueError(
                "--candidate-url must use SOURCE_CODE=URL, "
                f"got {raw_value!r}"
            )
        source_code, url = raw_value.split("=", 1)
        source_code = source_code.strip()
        url = url.strip()
        if not source_code or not url:
            raise ValueError(
                "--candidate-url requires a non-empty SOURCE_CODE and URL"
            )
        candidate_map.setdefault(source_code, []).append(url)
    return candidate_map


def _candidate_urls_for_source(
    source: dict[str, Any],
    candidate_map: dict[str, list[str]] | None = None,
) -> list[dict[str, str]]:
    source_code = str(source.get("sourceCode") or source.get("code") or "").strip()
    candidates: list[dict[str, str]] = []
    source_url = str(source.get("sourceUrl") or source.get("finalUrl") or "").strip()
    if source_url:
        candidates.append({"kind": "registered_source", "url": source_url})
    for url in (candidate_map or {}).get(source_code, []):
        candidates.append({"kind": "candidate_url", "url": url})

    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for candidate in candidates:
        url = candidate["url"]
        if url in seen:
            continue
        seen.add(url)
        deduped.append(candidate)
    return deduped


def _signal_summary_from_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    signals = evidence.get("signals") or {}
    keyword_hits = signals.get("keyword_hits") or {}
    return {
        "title": (evidence.get("page") or {}).get("title"),
        "msrpKeywordHits": keyword_hits.get("msrp") or [],
        "financeKeywordHits": keyword_hits.get("finance") or [],
        "configuratorKeywordHits": keyword_hits.get("configurator") or [],
        "jsonKeywordHits": keyword_hits.get("json") or [],
        "priceLikeSamples": signals.get("price_like_samples") or [],
        "ldJsonScriptCount": signals.get("ld_json_script_count") or 0,
        "candidateLinks": signals.get("candidate_links") or [],
        "headings": signals.get("headings") or [],
    }


def classify_price_signal(
    *,
    evidence: dict[str, Any],
    heuristics: dict[str, Any],
) -> dict[str, Any]:
    """Classify deterministic official MSRP page signals."""
    signals = evidence.get("signals") or {}
    keyword_hits = signals.get("keyword_hits") or {}
    price_samples = [
        str(sample)
        for sample in signals.get("price_like_samples") or []
        if str(sample).strip()
    ]
    msrp_hits = [
        str(hit) for hit in keyword_hits.get("msrp") or [] if str(hit).strip()
    ]
    finance_hits = [
        str(hit) for hit in keyword_hits.get("finance") or [] if str(hit).strip()
    ]
    campaign_hits_from_evidence = [
        str(hit) for hit in keyword_hits.get("campaign") or [] if str(hit).strip()
    ]
    page_semantics = str(heuristics.get("page_semantics") or "unknown")
    evidence_text = " ".join(
        [
            str(evidence.get("text_excerpt") or ""),
            " ".join(msrp_hits),
            " ".join(finance_hits),
        ]
    ).lower()
    campaign_hits = sorted(
        set(campaign_hits_from_evidence)
        | {keyword for keyword in CAMPAIGN_PRICE_KEYWORDS if keyword in evidence_text}
    )

    if price_samples and campaign_hits:
        return {
            "officialPriceSignalStatus": "campaign_or_net_price_signal",
            "recommendedAction": "route_to_campaign_or_net_price_pipeline_not_base_msrp",
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "reason": (
                "Official page includes price-like values, but promotion, "
                "bonus, subsidy, cash, or leasing context means it must not "
                "be treated as base MSRP."
            ),
            "nonMsrpSignalType": "campaign_or_net_price",
            "nonMsrpKeywordHits": campaign_hits,
        }
    if price_samples and page_semantics == "base_msrp":
        return {
            "officialPriceSignalStatus": "price_signal_present",
            "recommendedAction": "repair_selector_and_run_dryrun",
            "dryrunCandidateEligible": True,
            "officialIngestEligible": False,
            "reason": (
                "Official page text includes MSRP semantics and price-like "
                "values; validate through dryrun before ingest."
            ),
        }
    if price_samples and (page_semantics == "mixed" or (msrp_hits and finance_hits)):
        return {
            "officialPriceSignalStatus": "ambiguous_price_signal",
            "recommendedAction": "manual_review_price_semantics",
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "reason": (
                "Price-like values are present, but MSRP and finance wording "
                "must be separated before selector repair."
            ),
        }
    if price_samples and finance_hits and not msrp_hits:
        return {
            "officialPriceSignalStatus": "finance_only_signal",
            "recommendedAction": "avoid_finance_selector_find_msrp_source",
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "reason": "Detected price-like values only in finance/leasing context.",
        }
    if not price_samples:
        return {
            "officialPriceSignalStatus": "no_price_signal",
            "recommendedAction": "do_not_promote_find_price_list_or_api",
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "reason": "No price-like values were detected in visible page text.",
        }
    return {
        "officialPriceSignalStatus": "unknown_price_signal",
        "recommendedAction": "manual_review_price_semantics",
        "dryrunCandidateEligible": False,
        "officialIngestEligible": False,
        "reason": "Price-like values were detected, but MSRP semantics are unclear.",
    }


def _request_candidate(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: float,
) -> requests.Response:
    return session.get(url, allow_redirects=True, timeout=timeout_seconds)


def inspect_candidate_url(
    source: dict[str, Any],
    *,
    candidate: dict[str, str],
    session: requests.Session,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    source_code = str(source.get("sourceCode") or source.get("code") or "").strip()
    country_code = str(source.get("countryCode") or source.get("country") or "").lower()
    candidate_url = str(candidate.get("url") or "").strip()
    candidate_kind = str(candidate.get("kind") or "candidate_url")
    started = time.perf_counter()

    response: requests.Response | None = None
    error: str | None = None
    error_type: str | None = None
    if candidate_url:
        try:
            response = _request_candidate(
                session,
                candidate_url,
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
    status_code = (
        int(getattr(response, "status_code", 0) or 0)
        if response is not None
        else None
    )
    final_url = (
        str(getattr(response, "url", "") or candidate_url)
        if response is not None
        else candidate_url
    )
    headers = (
        dict(getattr(response, "headers", {}) or {})
        if response is not None
        else {}
    )
    text = (
        str(getattr(response, "text", "") or "")
        if response is not None
        else ""
    )
    host = _host_from_url(final_url or candidate_url)

    base_item: dict[str, Any] = {
        "countryCode": country_code,
        "sourceCode": source_code,
        "brand": source.get("brand"),
        "candidateKind": candidate_kind,
        "candidateUrl": candidate_url,
        "host": host,
        "httpStatus": status_code,
        "finalUrl": final_url,
        "contentType": headers.get("content-type") or headers.get("Content-Type"),
        "elapsedMs": elapsed_ms,
        "dryrunFailureReason": source.get("failureReason"),
        "dryrunRecommendedStrategy": source.get("recommendedStrategy"),
    }

    if not candidate_url:
        return {
            **base_item,
            "fetchStatus": "missing_candidate_url",
            "officialPriceSignalStatus": "missing_candidate_url",
            "recommendedAction": "update_source_url",
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "signalSummary": {},
            "heuristics": {},
        }

    probe_classification = classify_probe_result(
        url=candidate_url,
        status_code=status_code,
        final_url=final_url,
        headers=headers,
        text_sample=" ".join(text.split())[:500],
        error=error,
        error_type=error_type,
    )
    if error or probe_classification["probeStatus"] != "fetchable":
        probe_status = str(probe_classification.get("probeStatus") or "probe_error")
        return {
            **base_item,
            "fetchStatus": probe_status,
            "officialPriceSignalStatus": (
                "access_blocked"
                if probe_status in {"anti_bot_blocked", "forbidden_403"}
                else probe_status
            ),
            "recommendedAction": probe_classification.get(
                "recommendedAction",
                "manual_review_source_access",
            ),
            "dryrunCandidateEligible": False,
            "officialIngestEligible": False,
            "signalSummary": {},
            "heuristics": {},
            **({"error": probe_classification.get("error")} if probe_classification.get("error") else {}),
        }

    evidence = build_page_evidence(
        html=text,
        url=candidate_url,
        metadata={
            "requested_url": candidate_url,
            "final_url": final_url,
            "status_code": status_code,
            "content_type": base_item["contentType"],
        },
    )
    heuristics = analyze_page_heuristics(evidence)
    signal_classification = classify_price_signal(
        evidence=evidence,
        heuristics=heuristics,
    )
    return {
        **base_item,
        "fetchStatus": "fetchable",
        **signal_classification,
        "signalSummary": _signal_summary_from_evidence(evidence),
        "heuristics": {
            "pageSemantics": heuristics.get("page_semantics"),
            "recommendedExtractor": heuristics.get("recommended_extractor"),
            "powertrainGranularity": heuristics.get("powertrain_granularity"),
            "confidence": heuristics.get("confidence"),
            "shouldUseLlmInPipeline": heuristics.get("should_use_llm_in_pipeline"),
            "selectorHints": heuristics.get("selector_hints") or {},
            "risks": heuristics.get("risks") or [],
            "recommendation": heuristics.get("recommendation"),
        },
    }


def _increment(target: dict[str, int], value: Any) -> None:
    key = str(value or "unknown")
    target[key] = target.get(key, 0) + 1


def summarize_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    signal_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    fetch_counts: dict[str, int] = {}
    dryrun_candidate_count = 0
    official_ingest_count = 0
    for item in items:
        _increment(signal_counts, item.get("officialPriceSignalStatus"))
        _increment(action_counts, item.get("recommendedAction"))
        _increment(fetch_counts, item.get("fetchStatus"))
        if item.get("dryrunCandidateEligible"):
            dryrun_candidate_count += 1
        if item.get("officialIngestEligible"):
            official_ingest_count += 1
    return {
        "candidateUrlCount": len(items),
        "fetchStatusCounts": fetch_counts,
        "officialPriceSignalCounts": signal_counts,
        "recommendedActionCounts": action_counts,
        "dryrunCandidateEligibleCount": dryrun_candidate_count,
        "officialIngestEligibleCount": official_ingest_count,
        "noPriceSignalCount": signal_counts.get("no_price_signal", 0),
        "accessBlockedCount": signal_counts.get("access_blocked", 0),
    }


def build_price_signal_report(
    backlog: dict[str, Any],
    *,
    session: requests.Session,
    candidate_map: dict[str, list[str]] | None = None,
    timeout_seconds: float = 20.0,
    include_transient: bool = False,
) -> dict[str, Any]:
    sources = source_issues_from_backlog(
        backlog,
        include_transient=include_transient,
    )
    items: list[dict[str, Any]] = []
    for source in sources:
        for candidate in _candidate_urls_for_source(source, candidate_map):
            items.append(
                inspect_candidate_url(
                    source,
                    candidate=candidate,
                    session=session,
                    timeout_seconds=timeout_seconds,
                )
            )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": _utc_now_iso(),
        "backlogRunId": backlog.get("runId"),
        "includeTransient": include_transient,
        "officialSourceRequiredForIngest": True,
        "summary": {
            "sourceRepairIssueCount": int(backlog.get("sourceRepairIssueCount") or 0),
            "transientRegressionCount": int(backlog.get("transientRegressionCount") or 0),
            "inspectedSourceCount": len(sources),
            **summarize_items(items),
        },
        "items": items,
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    summary = report.get("summary") or {}
    lines = [
        "# MSRP Official Price Signal Audit",
        "",
        f"Generated: {report.get('generatedAt') or '-'}",
        f"Backlog run: {report.get('backlogRunId') or '-'}",
        "Policy: price signals create dryrun candidates only; official ingest still requires validation/review",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Inspected sources | {summary.get('inspectedSourceCount', 0)} |",
        f"| Candidate URLs | {summary.get('candidateUrlCount', 0)} |",
        f"| Dryrun candidate eligible | {summary.get('dryrunCandidateEligibleCount', 0)} |",
        f"| Official ingest eligible | {summary.get('officialIngestEligibleCount', 0)} |",
        f"| No price signal | {summary.get('noPriceSignalCount', 0)} |",
        f"| Access blocked | {summary.get('accessBlockedCount', 0)} |",
        "",
        "| Country | Source | Candidate | HTTP | Signal | Recommended action | URL |",
        "|---|---|---|---:|---|---|---|",
    ]
    for item in report.get("items") or []:
        lines.append(
            "| {country} | `{source}` | {kind} | {status} | {signal} | {action} | {url} |".format(
                country=str(item.get("countryCode") or "").upper(),
                source=item.get("sourceCode") or "-",
                kind=item.get("candidateKind") or "-",
                status=item.get("httpStatus") or "-",
                signal=item.get("officialPriceSignalStatus") or "-",
                action=item.get("recommendedAction") or "-",
                url=item.get("finalUrl") or item.get("candidateUrl") or "-",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    backlog_path: str | None = None,
    out_dir: str | None = None,
    *,
    candidate_url_args: list[str] | None = None,
    timeout_seconds: float = 20.0,
    include_transient: bool = False,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    backlog = _load_json(Path(backlog_path or DEFAULT_BACKLOG_PATH))
    output_dir = Path(out_dir).resolve() if out_dir else DEFAULT_OUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_map = _parse_candidate_url_args(candidate_url_args)
    owns_session = session is None
    current_session = session or requests.Session()
    current_session.headers.update(DEFAULT_HEADERS)
    try:
        report = build_price_signal_report(
            backlog,
            session=current_session,
            candidate_map=candidate_map,
            timeout_seconds=timeout_seconds,
            include_transient=include_transient,
        )
    finally:
        if owns_session:
            current_session.close()

    json_path = output_dir / "msrp_official_price_signal_audit.json"
    md_path = output_dir / "msrp_official_price_signal_audit.md"
    json_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_markdown(report, md_path)
    print(f"[price-signal] JSON: {json_path}")
    print(f"[price-signal] Markdown: {md_path}")
    print(
        "[price-signal] "
        f"{report['summary']['candidateUrlCount']} candidate URLs inspected; "
        f"dryrunCandidates={report['summary']['dryrunCandidateEligibleCount']}; "
        f"officialIngestEligible={report['summary']['officialIngestEligibleCount']}"
    )
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect official MSRP source candidates for price signals.",
    )
    parser.add_argument("--backlog", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument(
        "--candidate-url",
        action="append",
        default=[],
        help=(
            "Additional official candidate URL as SOURCE_CODE=URL. "
            "Repeat for multiple candidates."
        ),
    )
    parser.add_argument(
        "--include-transient",
        action="store_true",
        help="Also inspect transient regressions with last-known-good evidence.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run(
        backlog_path=args.backlog,
        out_dir=args.out_dir,
        candidate_url_args=args.candidate_url,
        timeout_seconds=max(1.0, args.timeout_seconds),
        include_transient=args.include_transient,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
