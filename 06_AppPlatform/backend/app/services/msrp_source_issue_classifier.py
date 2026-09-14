"""Canonical classification for failed official MSRP sources.

The dry-run dashboard, Hermes governance API, and repair-backlog generator all
consume the same metadata so a source failure has one stable explanation.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

MSRP_SOURCE_ISSUE_FIELDS = (
    "failureReason",
    "recommendedStrategy",
    "originalFailureReason",
    "originalRecommendedStrategy",
    "issueClass",
    "sourceLifecycleStatus",
    "blockingDisposition",
    "likelyCause",
    "recommendedAction",
)

_OFFICIAL_UNAVAILABLE_URL_MARKERS = (
    "ikke-tilgjengelig",
    "ikke_tilgjengelig",
    "not-available",
    "no-longer-available",
    "model-unavailable",
    "unavailable",
    "ej-tillganglig",
    "ej-tillgänglig",
    "inte-tillganglig",
    "inte-tillgänglig",
)
_OFFICIAL_UNAVAILABLE_TEXT_MARKERS = (
    "model is not available",
    "model is unavailable",
    "vehicle is not available",
    "vehicle is unavailable",
    "vehicle is no longer available",
    "model is no longer available",
    "modellen är inte tillgänglig",
    "modellen er ikke tilgjengelig",
)
_EXTRACTOR_FAILURE_REASONS = frozenset(
    {
        "extractor_error",
        "extractor_failed",
        "extraction_failed",
        "json_ld_empty",
        "no_observation_extracted",
        "parser_error",
        "selector_empty",
    }
)
_OFFICIAL_UNAVAILABLE_REASONS = frozenset(
    {"model_not_currently_available", "official_model_unavailable"}
)
_FAILED_SOURCE_STATUSES = frozenset(
    {"empty", "error", "exception", "fail", "failed", "rejected"}
)


def _normalized_url_path(value: str) -> str:
    path = urlparse(value).path or "/"
    return path.rstrip("/") or "/"


def _normalized_host(value: str) -> str:
    host = (urlparse(value).hostname or "").casefold().strip(".")
    return host.removeprefix("www.")


def _is_homepage_redirect(source_url: str, final_url: str) -> bool:
    if not source_url or not final_url:
        return False
    source_host = _normalized_host(source_url)
    final_host = _normalized_host(final_url)
    if not source_host or source_host != final_host:
        return False
    return (
        _normalized_url_path(source_url) != "/"
        and _normalized_url_path(final_url) == "/"
    )


def _is_official_unavailable(source: dict[str, Any]) -> bool:
    urls = " ".join(
        unquote(str(source.get(key) or "")).casefold()
        for key in ("finalUrl", "final_url", "sourceUrl", "source_url")
    )
    if any(marker in urls for marker in _OFFICIAL_UNAVAILABLE_URL_MARKERS):
        return True

    page_text = " ".join(
        str(source.get(key) or "").casefold()
        for key in (
            "availabilityMessage",
            "error",
            "extractorError",
            "officialAvailabilityMessage",
            "pageTitle",
        )
    )
    return any(marker in page_text for marker in _OFFICIAL_UNAVAILABLE_TEXT_MARKERS)


def _http_status(source: dict[str, Any]) -> int | None:
    try:
        return int(source.get("httpStatus") or source.get("http_status"))
    except (TypeError, ValueError):
        return None


def _failed_status(source: dict[str, Any]) -> bool:
    status = str(source.get("rawStatus") or source.get("status") or "").casefold()
    return status in _FAILED_SOURCE_STATUSES


def is_msrp_source_issue(source: dict[str, Any]) -> bool:
    """Return whether a source row represents a classifiable failed probe."""
    return bool(
        source.get("failureReason")
        or _http_status(source) == 403
        or (source.get("extractorError") and _failed_status(source))
    )


def classify_msrp_source_issue(source: dict[str, Any]) -> dict[str, Any]:
    """Return canonical, user-facing governance metadata for one source row."""
    source_url = str(source.get("sourceUrl") or source.get("source_url") or "")
    final_url = str(source.get("finalUrl") or source.get("final_url") or "")
    original_reason = str(source.get("failureReason") or "unknown")
    original_strategy = str(
        source.get("recommendedStrategy")
        or "diagnose_with_msrp_page_analyzer"
    )
    first_reason = str(source.get("originalFailureReason") or original_reason)
    first_strategy = str(
        source.get("originalRecommendedStrategy") or original_strategy
    )
    metadata: dict[str, Any] = {
        "failureReason": original_reason,
        "recommendedStrategy": original_strategy,
    }

    if (
        original_reason in _OFFICIAL_UNAVAILABLE_REASONS
        or _is_official_unavailable(source)
    ):
        metadata.update(
            {
                "failureReason": "official_model_unavailable",
                "recommendedStrategy": (
                    "mark_model_unavailable_or_replace_official_source"
                ),
                "issueClass": "source_lifecycle",
                "sourceLifecycleStatus": "official_unavailable",
                "blockingDisposition": (
                    "do_not_ingest_without_current_official_source"
                ),
                "likelyCause": (
                    "The official destination says this model is unavailable."
                ),
                "recommendedAction": (
                    "Confirm current national availability; mark the source "
                    "lifecycle or replace it with a current official source."
                ),
            }
        )
    elif _http_status(source) == 403 or original_reason == "forbidden_403":
        metadata.update(
            {
                "failureReason": "forbidden_403",
                "recommendedStrategy": "manual_review_or_proxy_required",
                "issueClass": "access_control",
                "blockingDisposition": "manual_or_proxy_required",
                "likelyCause": "The official site denied automated access.",
                "recommendedAction": (
                    "Use an approved proxy, cookie, or manual official-source "
                    "review; do not substitute third-party prices."
                ),
            }
        )
    elif (
        original_reason == "homepage_redirect"
        or _is_homepage_redirect(source_url, final_url)
    ):
        metadata.update(
            {
                "failureReason": "homepage_redirect",
                "recommendedStrategy": (
                    "verify_model_current_availability_or_update_source_url"
                ),
                "issueClass": "source_lifecycle",
                "sourceLifecycleStatus": "homepage_redirect",
                "blockingDisposition": (
                    "do_not_ingest_without_current_official_source"
                ),
                "likelyCause": (
                    "The official model URL redirected to the national homepage."
                ),
                "recommendedAction": (
                    "Verify current national availability or replace the "
                    "official model URL."
                ),
            }
        )
    elif (
        original_reason in _EXTRACTOR_FAILURE_REASONS
        or (source.get("extractorError") and _failed_status(source))
    ):
        metadata.update(
            {
                "issueClass": "extractor_strategy",
                "blockingDisposition": "repair_extractor_before_ingest",
                "likelyCause": (
                    "The official page was reached, but the configured "
                    "extractor produced no usable MSRP observation."
                ),
                "recommendedAction": (
                    "Repair the selector or extractor strategy against the "
                    "official page, then rerun the dry-run."
                ),
            }
        )

    if metadata["failureReason"] != first_reason:
        metadata["originalFailureReason"] = first_reason
    if metadata["recommendedStrategy"] != first_strategy:
        metadata["originalRecommendedStrategy"] = first_strategy
    return metadata


def enrich_msrp_source_issue(source: dict[str, Any]) -> dict[str, Any]:
    """Return *source* with canonical issue metadata overlaid."""
    return {**source, **classify_msrp_source_issue(source)}


def _normalized_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for name, count in value.items():
        label = str(name or "").strip() or "unknown"
        try:
            amount = int(count or 0)
        except (TypeError, ValueError):
            continue
        if amount > 0:
            normalized[label] = normalized.get(label, 0) + amount
    return normalized


def _replace_count(
    counts: dict[str, int],
    original: str,
    canonical: str,
) -> bool:
    if original == canonical:
        return True
    if counts.get(original, 0) <= 0:
        return False
    counts[original] -= 1
    if counts[original] <= 0:
        counts.pop(original, None)
    counts[canonical] = counts.get(canonical, 0) + 1
    return True


def effective_msrp_country_issue_maps(
    country: dict[str, Any],
) -> tuple[dict[str, int], dict[str, int]]:
    """Return failure and strategy counts with source-level refinements applied.

    Aggregate counts are retained when a report carries only sampled source
    rows. When all failed rows are present, the detailed rows become the source
    of truth.
    """
    sources = [
        source
        for source in country.get("sources") or []
        if isinstance(source, dict) and is_msrp_source_issue(source)
    ]
    existing_failures = _normalized_count_map(country.get("failureBreakdown"))
    existing_strategies = _normalized_count_map(
        country.get("strategyRecommendations")
    )
    if not sources:
        return existing_failures, existing_strategies

    detailed_failures: dict[str, int] = {}
    detailed_strategies: dict[str, int] = {}
    classified: list[dict[str, Any]] = []
    for source in sources:
        issue = classify_msrp_source_issue(source)
        classified.append(issue)
        reason = str(issue.get("failureReason") or "unknown")
        strategy = str(
            issue.get("recommendedStrategy")
            or "diagnose_with_msrp_page_analyzer"
        )
        detailed_failures[reason] = detailed_failures.get(reason, 0) + 1
        detailed_strategies[strategy] = detailed_strategies.get(strategy, 0) + 1

    if not existing_failures or len(sources) >= sum(existing_failures.values()):
        return detailed_failures, detailed_strategies

    failures = dict(existing_failures)
    strategies = dict(existing_strategies)
    for issue in classified:
        _replace_count(
            failures,
            str(
                issue.get("originalFailureReason")
                or issue.get("failureReason")
                or "unknown"
            ),
            str(issue.get("failureReason") or "unknown"),
        )
        _replace_count(
            strategies,
            str(
                issue.get("originalRecommendedStrategy")
                or issue.get("recommendedStrategy")
                or "diagnose_with_msrp_page_analyzer"
            ),
            str(
                issue.get("recommendedStrategy")
                or "diagnose_with_msrp_page_analyzer"
            ),
        )
    return failures, strategies
