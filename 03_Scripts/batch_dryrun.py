#!/usr/bin/env python3
"""Batch dry-run all draft sources for specified countries.

Runs each source individually to isolate failures, and produces
a summary report.
"""
import json
import logging
import multiprocessing
import os
import queue
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator
from urllib.parse import urlparse

# Ensure jato_scraper is importable
_script_dir = str(Path(__file__).resolve().parent)
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)
_toolkit_dir = str(
    Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
)
if _toolkit_dir not in sys.path:
    sys.path.insert(0, _toolkit_dir)
_hermes_script_dir = str(Path(__file__).resolve().parent / "hermes")
if _hermes_script_dir not in sys.path:
    sys.path.insert(0, _hermes_script_dir)

from pipeline_status_writer import write_pipeline_status

try:
    from msrp_dryrun_aggregate import (
        _write_source_repair_backlog as _write_v3_source_repair_backlog,
    )
except ImportError:  # pragma: no cover - keeps dryrun usable in stripped script contexts.
    _write_v3_source_repair_backlog = None

SOURCE_RESULT_DIAGNOSTIC_KEYS = (
    "sourceUrl",
    "brand",
    "error",
    "extractorName",
    "extractorVersion",
    "coverageLevel",
    "auditStatus",
    "attemptedStrategies",
    "winningStrategy",
    "extractorError",
    "httpStatus",
    "finalUrl",
    "financeObservationCandidates",
    "financeMonthlyPaymentCount",
    "financeSemanticsCounts",
    "financeTypeCounts",
    "sampleFinanceContexts",
    "rejectedReasons",
    "rejectedRules",
    "rejectionReasonCounts",
    "rejectionRuleCounts",
    "sampleRejectedObservations",
)


def _copy_source_result_diagnostics(src: dict, result_entry: dict) -> None:
    for key in SOURCE_RESULT_DIAGNOSTIC_KEYS:
        if key in src:
            result_entry[key] = src[key]

_TOOLKIT_ROOT = Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
_DRAFTS_DIR = _TOOLKIT_ROOT / "source_drafts" / "suv_only_country_model_top30"
STRICT_EXIT = os.getenv("JATO_STRICT_EXIT", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

BATCH_COUNTRIES = {
    "1": ["se", "hr"],
    "2": ["hu", "no", "at", "cz", "ch"],
    "all": ["se", "hr", "hu", "no", "at", "cz", "ch"],
}

log = logging.getLogger(__name__)


class SourceAttemptTimeoutError(TimeoutError):
    """Raised when an entire run_scrape attempt exceeds its hard budget."""


class _RunLogCapture(logging.Handler):
    """Capture per-source extractor diagnostics without changing normal logging."""

    def __init__(self, *, max_messages: int = 40) -> None:
        super().__init__(level=logging.INFO)
        self.max_messages = max_messages
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if len(self.messages) >= self.max_messages:
            return
        message = self.format(record)
        self.messages.append(message)

    def text(self) -> str:
        return "\n".join(self.messages)


@contextmanager
def _capture_source_logs(handler: logging.Handler) -> Iterator[None]:
    """Capture per-source scraper diagnostics, including Scrapling INFO logs."""
    root_logger = logging.getLogger()
    target_loggers = [
        logging.getLogger("jato_scraper"),
        logging.getLogger("scrapling"),
    ]
    logger_states = [
        (logger, logger.level, logger.propagate)
        for logger in target_loggers
    ]

    root_logger.addHandler(handler)
    for logger in target_loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    try:
        yield
    finally:
        if handler in root_logger.handlers:
            root_logger.removeHandler(handler)
        for logger, level, propagate in logger_states:
            if handler in logger.handlers:
                logger.removeHandler(handler)
            logger.setLevel(level)
            logger.propagate = propagate


def _is_child_run() -> bool:
    return os.getenv("JATO_MSRP_CHILD_RUN", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _resolve_scraper_functions() -> tuple[Callable, Callable]:
    from jato_scraper.config_loader import load_all_sources
    from jato_scraper.runner import run_scrape

    return load_all_sources, run_scrape


def _promoted_code_for_draft(code: str) -> str:
    if not code.endswith("_draft_scrapling"):
        return code
    return code.replace("_draft_scrapling", "_scrapling")


def _failure_classification(
    failure_reason: str | None,
    recommended_strategy: str | None,
    severity: str,
) -> dict:
    return {
        "failureReason": failure_reason,
        "recommendedStrategy": recommended_strategy,
        "severity": severity,
    }


def _is_runner_browser_launch_failure(error_lower: str) -> bool:
    return (
        "browsertype.launch" in error_lower
        or "launch_persistent_context" in error_lower
        or (
            "targetclosederror" in error_lower
            and "browser has been closed" in error_lower
        )
        or (
            "kill eperm" in error_lower
            and "playwright_chromiumdev_profile" in error_lower
        )
    )


def _classify_dryrun_failure(
    src: dict,
    exception: Exception | None = None,
) -> dict:
    """Classify a dry-run failure and recommend next strategy."""
    status = str(src.get("status", "")).lower()
    error = str(
        src.get("error", "")
        or src.get("extractorError", "")
        or (str(exception) if exception else "")
    )
    try:
        valid = int(src.get("valid") or 0)
    except (TypeError, ValueError):
        valid = 0
    try:
        extracted = int(src.get("extracted") or 0)
    except (TypeError, ValueError):
        extracted = 0
    error_lower = error.lower()
    source_url = str(src.get("sourceUrl") or src.get("source_url") or "")
    final_url = str(src.get("finalUrl") or src.get("final_url") or "")
    http_status_raw = src.get("httpStatus") or src.get("http_status")
    try:
        http_status = int(http_status_raw) if http_status_raw is not None else None
    except (TypeError, ValueError):
        http_status = None

    if _is_runner_browser_launch_failure(error_lower):
        return _failure_classification(
            "runner_browser_launch_failed",
            "pipeline_error_not_source_error",
            "error",
        )
    if "404-page" in final_url.lower() or "/404" in final_url.lower():
        return _failure_classification("source_url_not_found", "update_source_url", "error")
    if (
        "anti_bot_access_denied" in error_lower
        or (
            "access denied" in error_lower
            and (
                "permission to access" in error_lower
                or "errors.edgesuite.net" in error_lower
                or "akamai" in error_lower
            )
        )
    ):
        return _failure_classification(
            "anti_bot_access_denied",
            "manual_review_or_proxy_required",
            "error",
        )
    if http_status == 403:
        return _failure_classification("forbidden_403", "manual_review_or_proxy_required", "error")
    if http_status == 404:
        return _failure_classification("source_url_not_found", "update_source_url", "error")
    if http_status and http_status >= 400:
        return _failure_classification("http_error", "check_source_url_or_site_status", "error")
    if valid > 0 and status not in {"empty", "error", "exception"}:
        return _failure_classification(None, None, "info")
    if _looks_like_discontinued_model_url(final_url):
        return {
            "failureReason": "model_not_currently_available",
            "recommendedStrategy": "exclude_or_replace_discontinued_model",
            "severity": "info",
        }
    if _redirected_to_brand_homepage(source_url, final_url):
        return {
            "failureReason": "source_url_redirected_to_homepage",
            "recommendedStrategy": "update_source_url_or_confirm_model_availability",
            "severity": "warning",
        }
    if (
        "could not resolve host" in error_lower
        or "failed to resolve" in error_lower
        or "nameresolutionerror" in error_lower
        or "nodename nor servname" in error_lower
        or "err_name_not_resolved" in error_lower
    ):
        return _failure_classification("dns_resolution_failed", "retry_or_check_dns", "warning")
    if (
        "err_internet_disconnected" in error_lower
        or "internet disconnected" in error_lower
        or "network is unreachable" in error_lower
        or "connection reset" in error_lower
        or "connection refused" in error_lower
        or "connection closed" in error_lower
        or "err_connection_closed" in error_lower
        or "failed to load" in error_lower
    ):
        return _failure_classification("network_unavailable", "retry_network_or_proxy", "warning")
    if "403" in error_lower or "forbidden" in error_lower:
        return _failure_classification("forbidden_403", "manual_review_or_proxy_required", "error")
    if "no plausible trim-overview msrp" in error_lower:
        return _failure_classification("dynamic_price_not_ready", "retry_or_reduce_concurrency", "warning")
    if "waiting for" in error_lower or "playwright" in error_lower:
        return _failure_classification(
            "js_required_or_selector_timeout",
            "try_playwright_card_flow",
            "warning",
        )
    if "fetch_failed" in error_lower:
        return _failure_classification("http_error", "check_source_url_or_site_status", "error")
    if "timeout" in error_lower or "timed out" in error_lower:
        return _failure_classification("http_timeout", "retry_or_reduce_concurrency", "warning")

    if exception or status in ("exception", "error"):
        if "waiting for" in error_lower or "playwright" in error_lower:
            return _failure_classification(
                "js_required_or_selector_timeout",
                "try_playwright_card_flow",
                "warning",
            )
        if "timeout" in error_lower:
            return _failure_classification("http_timeout", "retry_or_reduce_concurrency", "warning")
        if "403" in error_lower or "forbidden" in error_lower:
            return _failure_classification("forbidden_403", "manual_review_or_proxy_required", "error")
        if "selector" in error_lower or "no elements" in error_lower or "TODO_SELECTOR" in error:
            return _failure_classification(
                "selector_empty",
                "try_scrapling_dynamic_or_playwright",
                "warning",
            )
        if "502" in error_lower or "503" in error_lower or "bad gateway" in error_lower:
            return _failure_classification(
                "db_or_backend_write_failed",
                "pipeline_error_not_source_error",
                "error",
            )
        return _failure_classification("unknown", "diagnose_with_msrp_page_analyzer", "warning")

    if status == "empty":
        if "TODO_SELECTOR" in error:
            return _failure_classification(
                "selector_empty",
                "try_scrapling_dynamic_or_playwright",
                "warning",
            )
        if "selector" in error_lower or "no elements" in error_lower:
            return _failure_classification(
                "selector_empty",
                "try_scrapling_dynamic_or_playwright",
                "warning",
            )
        if "json" in error_lower and ("noth" in error_lower or "falling back" in error_lower):
            return _failure_classification("json_ld_empty", "try_css_or_attr_json", "warning")
        return _failure_classification(
            "no_observation_extracted",
            "diagnose_with_msrp_page_analyzer",
            "warning",
        )

    if extracted > 0 and valid == 0:
        rejected_reasons = [str(r).lower() for r in src.get("rejectedReasons", [])]
        rejected_rules = [str(r).lower() for r in src.get("rejectedRules", [])]
        rejection_rule_counts = src.get("rejectionRuleCounts") or {}
        if isinstance(rejection_rule_counts, dict):
            rejected_rules.extend(str(r).lower() for r in rejection_rule_counts)
        if any("currency" in r for r in rejected_reasons):
            return _failure_classification("currency_mismatch", "check_default_currency", "warning")
        if (
            any(r == "price_range" for r in rejected_rules)
            or any(
                ("price" in r or "msrp_value" in r)
                and ("range" in r or "out" in r or "<" in r or ">" in r)
                for r in rejected_reasons
            )
        ):
            return _failure_classification(
                "price_out_of_range",
                "check_currency_and_price_semantics",
                "warning",
            )
        return _failure_classification(
            "validation_rejected_all",
            "review_validation_rules",
            "warning",
        )

    return _failure_classification("unknown", "diagnose_with_msrp_page_analyzer", "info")


def _looks_like_discontinued_model_url(url: str) -> bool:
    """Detect official out-of-range model pages that should not be treated as selector bugs."""
    normalized = url.lower()
    return any(
        marker in normalized
        for marker in (
            "ikke-tilgjengelig",
            "ikke_tilgjengelig",
            "ikke-lenger-tilgjengelig",
            "not-available",
            "not_available",
            "discontinued",
            "utgar",
            "utgår",
            "utga",
            "utgå",
        )
    )


def _redirected_to_brand_homepage(source_url: str, final_url: str) -> bool:
    """Return True when a model source resolves to the same host's homepage."""
    if not source_url or not final_url:
        return False
    source = urlparse(source_url)
    final = urlparse(final_url)
    if not source.netloc or not final.netloc:
        return False
    if source.netloc.lower() != final.netloc.lower():
        return False
    source_path = (source.path or "/").rstrip("/") or "/"
    final_path = (final.path or "/").rstrip("/") or "/"
    return source_path != "/" and final_path == "/"


def _is_passing_result(result: dict) -> bool:
    """Return True only when a dryrun has valid data and no classified failure."""
    status = str(result.get("rawStatus") or result.get("status") or "").lower()
    try:
        valid = int(result.get("valid") or 0)
    except (TypeError, ValueError):
        valid = 0
    return valid > 0 and not result.get("failureReason") and status not in {"empty", "error", "exception"}


def _status_for_pass_pct(pass_pct: float) -> str:
    if pass_pct >= 90:
        return "success"
    if pass_pct >= 50:
        return "degraded"
    return "failure"


def _gate_status(pass_pct: float, threshold: int) -> str:
    return "allowed" if pass_pct >= threshold else "blocked"


def _failure_breakdown(results: list[dict]) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for result in results:
        reason = result.get("failureReason")
        if reason:
            breakdown[reason] = breakdown.get(reason, 0) + 1
    return breakdown


def _strategy_recommendations(results: list[dict]) -> dict[str, int]:
    recommendations: dict[str, int] = {}
    for result in results:
        strategy = result.get("recommendedStrategy")
        if strategy:
            recommendations[strategy] = recommendations.get(strategy, 0) + 1
    return recommendations


def _int_value(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _merge_count_maps(results: list[dict], key: str) -> dict[str, int]:
    merged: dict[str, int] = {}
    for result in results:
        value = result.get(key)
        if not isinstance(value, dict):
            continue
        for name, count in value.items():
            label = str(name or "").strip() or "unknown"
            merged[label] = merged.get(label, 0) + _int_value(count)
    return merged


def _finance_summary_from_results(results: list[dict]) -> dict[str, object]:
    return {
        "financeObservationCandidates": sum(
            _int_value(result.get("financeObservationCandidates"))
            for result in results
        ),
        "financeMonthlyPaymentCount": sum(
            _int_value(result.get("financeMonthlyPaymentCount"))
            for result in results
        ),
        "financeSemanticsCounts": _merge_count_maps(
            results,
            "financeSemanticsCounts",
        ),
        "financeTypeCounts": _merge_count_maps(results, "financeTypeCounts"),
    }


def _result_is_error(result: dict) -> bool:
    return str(result.get("rawStatus") or result.get("status") or "").lower() in {"error", "exception"}


def _result_is_empty(result: dict) -> bool:
    return str(result.get("rawStatus") or result.get("status") or "").lower() == "empty"


def _result_is_fail(result: dict) -> bool:
    return not _is_passing_result(result) and not _result_is_empty(result) and not _result_is_error(result)


_RETRYABLE_SOURCE_FAILURES = {
    "dns_resolution_failed",
    "http_timeout",
    "js_required_or_selector_timeout",
    "network_unavailable",
    "dynamic_price_not_ready",
}


def _source_attempt_limit() -> int:
    raw = os.getenv("JATO_MSRP_DRYRUN_SOURCE_ATTEMPTS", "2").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        log.warning(
            "Invalid JATO_MSRP_DRYRUN_SOURCE_ATTEMPTS=%r; using 2",
            raw,
        )
        return 2


def _source_retry_delay_seconds() -> float:
    raw = os.getenv("JATO_MSRP_DRYRUN_RETRY_DELAY_SECONDS", "2").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        log.warning(
            "Invalid JATO_MSRP_DRYRUN_RETRY_DELAY_SECONDS=%r; using 2",
            raw,
        )
        return 2.0


def _source_timeout_seconds() -> int:
    raw = os.getenv(
        "JATO_MSRP_DRYRUN_SOURCE_TIMEOUT_SECONDS",
        os.getenv("JATO_SOURCE_TIMEOUT_SECONDS", "180"),
    ).strip()
    try:
        return max(0, int(raw))
    except ValueError:
        log.warning(
            "Invalid MSRP source timeout %r; using 180",
            raw,
        )
        return 180


def _source_attempt_timeout_seconds(source_timeout_seconds: int) -> int:
    raw = os.getenv("JATO_MSRP_DRYRUN_ATTEMPT_TIMEOUT_SECONDS", "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            log.warning(
                "Invalid JATO_MSRP_DRYRUN_ATTEMPT_TIMEOUT_SECONDS=%r; "
                "using source timeout + 60",
                raw,
            )
    if source_timeout_seconds <= 0:
        return 0
    return source_timeout_seconds + 60


def _run_scrape_once(
    run_scrape: Callable,
    code: str,
    *,
    source_timeout_seconds: int,
) -> tuple[dict, str]:
    log_capture = _RunLogCapture()
    log_capture.setFormatter(logging.Formatter("%(levelname)s %(name)s — %(message)s"))
    with _capture_source_logs(log_capture):
        summary = run_scrape(
            source_codes=[code],
            dry_run=True,
            source_timeout_seconds=source_timeout_seconds,
        )
    return summary, log_capture.text()


def _run_scrape_attempt_child(
    run_scrape: Callable,
    code: str,
    source_timeout_seconds: int,
    output_queue: multiprocessing.Queue,
) -> None:
    try:
        summary, captured_log_text = _run_scrape_once(
            run_scrape,
            code,
            source_timeout_seconds=source_timeout_seconds,
        )
        output_queue.put(("ok", summary, captured_log_text))
    except BaseException as exc:
        output_queue.put(("error", type(exc).__name__, str(exc)))


def _run_scrape_attempt(
    run_scrape: Callable,
    code: str,
    *,
    source_timeout_seconds: int,
    attempt_timeout_seconds: int,
) -> tuple[dict, str]:
    if attempt_timeout_seconds <= 0:
        return _run_scrape_once(
            run_scrape,
            code,
            source_timeout_seconds=source_timeout_seconds,
        )
    if "fork" not in multiprocessing.get_all_start_methods():
        log.warning(
            "Hard attempt timeout requires fork; running %s inline",
            code,
        )
        return _run_scrape_once(
            run_scrape,
            code,
            source_timeout_seconds=source_timeout_seconds,
        )

    ctx = multiprocessing.get_context("fork")
    output_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_run_scrape_attempt_child,
        args=(run_scrape, code, source_timeout_seconds, output_queue),
    )
    process.start()
    process.join(attempt_timeout_seconds)
    try:
        if process.is_alive():
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            raise SourceAttemptTimeoutError(
                f"source {code} exceeded {attempt_timeout_seconds}s "
                "run_scrape attempt timeout"
            )

        try:
            status, payload, extra = output_queue.get_nowait()
        except queue.Empty as exc:
            raise RuntimeError(
                f"source {code} attempt process exited with code "
                f"{process.exitcode} without a result"
            ) from exc
        if status == "ok":
            return payload, str(extra or "")
        raise RuntimeError(f"{payload}: {extra}".strip(": "))
    finally:
        output_queue.close()
        output_queue.join_thread()


def _source_result_is_retryable(result: dict, classification: dict) -> bool:
    if _is_passing_result(result):
        return False
    return classification.get("failureReason") in _RETRYABLE_SOURCE_FAILURES


def _summary_from_results(results: list[dict]) -> dict:
    total = len(results)
    pass_count = sum(1 for result in results if _is_passing_result(result))
    empty_count = sum(1 for result in results if _result_is_empty(result))
    error_count = sum(1 for result in results if _result_is_error(result))
    fail_count = sum(1 for result in results if _result_is_fail(result))
    pass_pct = round(pass_count / total * 100, 1) if total else 0.0
    return {
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
        "passPct": pass_pct,
        "status": _status_for_pass_pct(pass_pct),
        "failureBreakdown": _failure_breakdown(results),
        "strategyRecommendations": _strategy_recommendations(results),
        **_finance_summary_from_results(results),
    }


def _normalize_source_for_v3(result: dict, index: int, total: int) -> dict:
    status = "pass" if _is_passing_result(result) else ("empty" if _result_is_empty(result) else "fail")
    source_code = result.get("sourceCode") or result.get("code") or ""
    payload = dict(result)
    payload.update({
        "index": index,
        "totalInCountry": total,
        "sourceCode": source_code,
        "code": source_code,
        "status": status,
        "rawStatus": result.get("status"),
        "valid": int(result.get("valid") or 0),
        "extracted": int(result.get("extracted") or 0),
        "rejected": int(result.get("rejected") or 0),
        "elapsedSeconds": float(result.get("elapsedSeconds") or result.get("elapsed") or 0),
    })
    return payload


def _country_detail_from_results(country: str, results: list[dict]) -> dict:
    summary = _summary_from_results(results)
    failure_breakdown = summary["failureBreakdown"]
    top_reason = max(failure_breakdown, key=failure_breakdown.get) if failure_breakdown else None
    total = int(summary["total"])
    return {
        "countryCode": country,
        "total": total,
        "pass": int(summary["pass"]),
        "empty": int(summary["empty"]),
        "fail": int(summary["fail"]),
        "errors": int(summary["errors"]),
        "passPct": float(summary["passPct"]),
        "status": summary["status"],
        "topFailureReason": top_reason,
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": summary["strategyRecommendations"],
        **_finance_summary_from_results(results),
        "sources": [
            _normalize_source_for_v3(result, index, total)
            for index, result in enumerate(results, start=1)
        ],
        "completed": True,
    }


def _build_dryrun_report_payload(
    *,
    batch: str,
    countries: list[str],
    results: list[dict],
    run_id: str,
    generated_at: str,
) -> dict:
    normalized_countries = [
        country.strip().lower()
        for country in countries
        if country.strip()
    ]
    expected_countries = sorted(set(normalized_countries))
    observed_countries = sorted({
        str(result.get("country") or "").strip().lower()
        for result in results
        if str(result.get("country") or "").strip()
    })
    missing_countries = sorted(set(expected_countries) - set(observed_countries))
    duplicate_countries = sorted({
        country
        for country in expected_countries
        if normalized_countries.count(country) > 1
    })

    summary = _summary_from_results(results)
    gate_threshold = int(os.getenv("JATO_MSRP_MIN_DRYRUN_PASS_PCT", "70"))
    summary = {
        **summary,
        "gateThreshold": gate_threshold,
        "gateStatus": _gate_status(float(summary["passPct"]), gate_threshold),
    }

    countries_detail: list[dict] = []
    for country in sorted(set(expected_countries) | set(observed_countries)):
        country_results = [
            result
            for result in results
            if str(result.get("country") or "").strip().lower() == country
        ]
        if not country_results:
            countries_detail.append({
                "countryCode": country,
                "total": 0,
                "pass": 0,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 0.0,
                "status": "missing",
                "topFailureReason": None,
                "failureBreakdown": {},
                "strategyRecommendations": {},
                "financeObservationCandidates": 0,
                "financeMonthlyPaymentCount": 0,
                "financeSemanticsCounts": {},
                "financeTypeCounts": {},
                "sources": [],
                "completed": False,
            })
            continue
        countries_detail.append(_country_detail_from_results(country, country_results))

    return {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": run_id,
        "batch": batch,
        "countries": expected_countries,
        "expectedCountries": expected_countries,
        "observedCountries": observed_countries,
        "missingCountries": missing_countries,
        "duplicateCountries": duplicate_countries,
        "summary": summary,
        "countriesDetail": countries_detail,
        "results": results,
        "generatedAt": generated_at,
        "savedAt": generated_at,
        # Backward-compatible top-level fields for older diagnostics scripts.
        "total": summary["total"],
        "pass": summary["pass"],
        "empty": summary["empty"],
        "fail": summary["fail"],
        "errors": summary["errors"],
        "passPct": summary["passPct"],
        "failureBreakdown": summary["failureBreakdown"],
        "strategyRecommendations": summary["strategyRecommendations"],
        "financeObservationCandidates": summary["financeObservationCandidates"],
        "financeMonthlyPaymentCount": summary["financeMonthlyPaymentCount"],
        "financeSemanticsCounts": summary["financeSemanticsCounts"],
        "financeTypeCounts": summary["financeTypeCounts"],
    }


def _relative_to_repo(path: Path) -> str:
    repo_root = Path(__file__).resolve().parent.parent
    try:
        return str(path.resolve().relative_to(repo_root))
    except ValueError:
        return str(path)


def _write_dryrun_runs_index(report: dict, latest_path: Path, history_path: Path) -> None:
    index_path = latest_path.parent / "dryrun_runs_index.json"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    index_data: dict = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "updatedAt": now,
        "latestRunId": report.get("runId"),
        "runs": [],
    }
    if index_path.is_file():
        try:
            index_data = json.loads(index_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    summary = report.get("summary") or {}
    run_id = report.get("runId")
    run_entry = {
        "runId": run_id,
        "mode": "dryrun",
        "batch": report.get("batch", ""),
        "startedAt": report.get("generatedAt"),
        "finishedAt": now,
        "status": summary.get("status", "unknown"),
        "gateStatus": summary.get("gateStatus"),
        "gateThreshold": summary.get("gateThreshold"),
        "passPct": summary.get("passPct", 0.0),
        "total": summary.get("total", 0),
        "pass": summary.get("pass", 0),
        "empty": summary.get("empty", 0),
        "fail": summary.get("fail", 0),
        "errors": summary.get("errors", 0),
        "financeObservationCandidates": summary.get("financeObservationCandidates", 0),
        "financeMonthlyPaymentCount": summary.get("financeMonthlyPaymentCount", 0),
        "expectedCountryCount": len(report.get("expectedCountries") or []),
        "observedCountryCount": len(report.get("observedCountries") or []),
        "missingCountryCount": len(report.get("missingCountries") or []),
        "artifactPath": _relative_to_repo(history_path),
        "latestArtifactPath": _relative_to_repo(latest_path),
        "reportMdPath": f"hermes/reports/msrp_country_progress_{run_id}.md",
        "runDir": "",
        "logFile": "",
    }

    existing_runs = [
        run
        for run in index_data.get("runs", [])
        if run.get("runId") != run_id
    ]
    existing_runs.insert(0, run_entry)
    index_data.update({
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "updatedAt": now,
        "latestRunId": run_id,
        "runs": existing_runs[:100],
    })
    index_path.write_text(json.dumps(index_data, indent=2, ensure_ascii=False) + "\n")


def _write_source_repair_backlog_artifact(report: dict, report_dir: Path) -> None:
    if _write_v3_source_repair_backlog is None:
        log.warning("Source repair backlog writer is unavailable; skipping artifact")
        return
    _write_v3_source_repair_backlog(report, report_dir)


def _write_dryrun_status(
    countries: list[str],
    pass_count: int,
    empty_count: int,
    fail_count: int,
    error_count: int,
    total: int = 0,
    results: list | None = None,
) -> None:
    """Write dryrun status — either global status or per-country artifact."""
    import json as _json
    import os as _os
    from datetime import datetime as _datetime, timezone as _timezone

    child_run = _is_child_run()
    run_dir = _os.environ.get("JATO_MSRP_RUN_DIR", "")
    run_id = _os.environ.get("JATO_MSRP_RUN_ID", "")

    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0
    if pass_pct >= 90:
        status = "success"
    elif pass_pct >= 50:
        status = "degraded"
    else:
        status = "failure"

    # ── Phase 3: Child dryrun → write per-country artifact ──
    if child_run and run_dir and countries:
        country = countries[0]
        artifact_dir = Path(run_dir) / "countries"
        artifact_dir.mkdir(parents=True, exist_ok=True)

        total_ok = pass_count
        total_fail = fail_count + error_count

        failure_breakdown: dict[str, int] = {}
        strategy_recs: dict[str, int] = {}
        for r in (results or []):
            reason = r.get("failureReason")
            if reason:
                failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
            strat = r.get("recommendedStrategy")
            if strat:
                strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

        artifact = {
            "schemaVersion": "msrp_dryrun_country_v1",
            "runId": run_id,
            "country": country,
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": error_count,
            "passPct": pass_pct,
            "status": status,
            "failureBreakdown": failure_breakdown,
            "strategyRecommendations": strategy_recs,
            **_finance_summary_from_results(results or []),
            "results": results or [],
        }
        artifact_path = artifact_dir / f"{country}.json"
        artifact_path.write_text(_json.dumps(artifact, indent=2) + "\n")
        print(f"[status] Country artifact written to {artifact_path} (child run, skipped global)")
        return

    # ── Direct run: write global scheduled_fetch_status.json ──
    status_path = Path(__file__).resolve().parent / "logs" / "scheduled_fetch_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if status_path.exists():
        try:
            existing = _json.loads(status_path.read_text())
        except (_json.JSONDecodeError, OSError):
            existing = {}
    total_ok = pass_count
    total_fail = fail_count + error_count
    country_total = len(countries)
    existing["msrp_dryrun"] = {
        "lastRunAt": _datetime.now(_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "countryCount": country_total,
        "totalSources": total,
        "successCount": total_ok,
        "failureCount": total_fail,
        "passPct": pass_pct,
        "artifactPath": "03_Scripts/diagnostics/artifacts/dryrun_report.json",
        "schemaVersion": "msrp_dryrun_report_v3",
    }
    status_path.write_text(_json.dumps(existing, indent=2) + "\n")
    write_pipeline_status(
        pipeline_id="msrp_dryrun",
        status=status,
        records_processed=total,
        failed_count=total_fail,
        warning_count=empty_count,
        artifact_refs=["03_Scripts/diagnostics/artifacts/dryrun_report.json"],
        source="03_Scripts/batch_dryrun.py",
        message=f"passPct={pass_pct}%",
        extra={
            "countryCount": country_total,
            "successCount": total_ok,
            "passPct": pass_pct,
            "schemaVersion": "msrp_dryrun_report_v3",
        },
    )
    print(f"[status] msrp_dryrun={status} passPct={pass_pct}% written to {status_path}")


def _write_dryrun_status_best_effort(
    countries: list[str],
    pass_count: int,
    empty_count: int,
    fail_count: int,
    error_count: int,
    total: int = 0,
    results: list | None = None,
) -> None:
    try:
        _write_dryrun_status(
            countries,
            pass_count,
            empty_count,
            fail_count,
            error_count,
            total=total,
            results=results,
        )
    except Exception as exc:
        log.warning(
            "Dryrun status write failed; continuing report generation: %s",
            exc,
        )


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)
    if len(sys.argv) > 1 and sys.argv[1] in {"-h", "--help"}:
        print("Usage: batch_dryrun.py [all|1|2|country[,country...]]")
        print("")
        print("Examples:")
        print("  batch_dryrun.py no")
        print("  batch_dryrun.py se,fi,dk")
        print("  batch_dryrun.py all")
        return
    batch = sys.argv[1] if len(sys.argv) > 1 else "all"
    countries = BATCH_COUNTRIES.get(batch, batch.split(","))
    load_all_sources, run_scrape = _resolve_scraper_functions()

    # Load both promoted sources and draft sources
    promoted_codes = set(load_all_sources())
    draft_codes = load_all_sources(sources_dir=_DRAFTS_DIR)
    draft_codes = [c for c in draft_codes if c.endswith("_draft_scrapling")]
    target_codes = []
    skipped_promoted = []
    for code in draft_codes:
        promoted_code = _promoted_code_for_draft(code)
        if promoted_code in promoted_codes:
            skipped_promoted.append((code, promoted_code))
            continue
        # source code format: brand_model_COUNTRY_draft_scrapling
        # Extract country suffix: last segment before "_draft_scrapling"
        parts = code.replace("_draft_scrapling", "").rsplit("_", 1)
        if len(parts) >= 2:
            cc = parts[-1]
            if cc in countries:
                target_codes.append((cc, code))

    target_codes.sort()
    print(f"Batch {batch}: {len(target_codes)} sources across {countries}")
    if skipped_promoted:
        print(
            "Skipped "
            f"{len(skipped_promoted)} promoted draft(s) because matching "
            "production sources already exist."
        )
    print(f"{'='*70}\n")

    results = []
    pass_count = 0
    fail_count = 0
    empty_count = 0
    error_count = 0

    source_attempt_limit = _source_attempt_limit()
    source_retry_delay = _source_retry_delay_seconds()
    source_timeout_seconds = _source_timeout_seconds()
    attempt_timeout_seconds = _source_attempt_timeout_seconds(source_timeout_seconds)

    for i, (cc, code) in enumerate(target_codes, 1):
        t0 = time.time()
        source_attempts: list[dict] = []
        try:
            src = {}
            captured_log_text = ""
            classification = {}
            for attempt in range(1, source_attempt_limit + 1):
                attempt_t0 = time.time()
                attempt_exception: Exception | None = None
                try:
                    summary, captured_log_text = _run_scrape_attempt(
                        run_scrape,
                        code,
                        source_timeout_seconds=source_timeout_seconds,
                        attempt_timeout_seconds=attempt_timeout_seconds,
                    )
                    src = summary["sources"].get(code, {})
                except Exception as exc:
                    attempt_exception = exc
                    src = {
                        "status": "exception",
                        "valid": 0,
                        "extracted": 0,
                        "rejected": 0,
                        "error": str(exc),
                    }
                    captured_log_text = ""
                classification_src = src
                if captured_log_text and not (src.get("error") or src.get("extractorError")):
                    classification_src = {**src, "extractorError": captured_log_text}
                classification = _classify_dryrun_failure(
                    classification_src,
                    exception=attempt_exception,
                )

                status = src.get("status", "error")
                valid = src.get("valid", 0)
                extracted = src.get("extracted", 0)
                retry_probe = {
                    "status": status,
                    "valid": valid,
                    "failureReason": classification.get("failureReason"),
                }
                source_attempts.append({
                    "attempt": attempt,
                    "status": status,
                    "valid": valid,
                    "extracted": extracted,
                    "failureReason": classification.get("failureReason"),
                    "elapsedSeconds": round(time.time() - attempt_t0, 1),
                })

                if (
                    attempt < source_attempt_limit
                    and _source_result_is_retryable(retry_probe, classification)
                ):
                    print(
                        f"      retry {attempt + 1}/{source_attempt_limit} "
                        f"for {code}: {classification.get('failureReason')}"
                    )
                    if source_retry_delay > 0:
                        time.sleep(source_retry_delay)
                    continue
                break

            status = src.get("status", "error")
            valid = src.get("valid", 0)
            extracted = src.get("extracted", 0)
            rejected = src.get("rejected", 0)
            elapsed = time.time() - t0

            count_result = {
                "status": status,
                "valid": valid,
                "failureReason": classification.get("failureReason"),
            }
            if _is_passing_result(count_result):
                icon = "✅"
                pass_count += 1
            elif _result_is_empty(count_result):
                icon = "⬚"
                empty_count += 1
            elif _result_is_error(count_result):
                icon = "❌"
                error_count += 1
            else:
                icon = "⚠"
                fail_count += 1

            print(
                f"  [{i:3d}/{len(target_codes)}] {icon} {code:50s} "
                f"valid={valid} extracted={extracted} rejected={rejected} "
                f"({elapsed:.1f}s)"
            )
            result_entry = {
                "country": cc,
                "code": code,
                "status": status,
                "valid": valid,
                "extracted": extracted,
                "rejected": rejected,
                "elapsed": round(elapsed, 1),
            }
            if source_attempts:
                result_entry["sourceAttempts"] = source_attempts
            _copy_source_result_diagnostics(src, result_entry)
            if (
                classification.get("failureReason")
                and captured_log_text
                and not result_entry.get("extractorError")
            ):
                result_entry["extractorError"] = captured_log_text[:1000]
            if classification.get("failureReason"):
                result_entry["failureReason"] = classification["failureReason"]
                result_entry["recommendedStrategy"] = classification["recommendedStrategy"]
                result_entry["severity"] = classification["severity"]
            results.append(result_entry)
        except Exception as e:
            elapsed = time.time() - t0
            classification = _classify_dryrun_failure({}, exception=e)
            print(
                f"  [{i:3d}/{len(target_codes)}] ❌ {code:50s} "
                f"ERROR: {e!s:.60s}"
            )
            results.append({
                "country": cc,
                "code": code,
                "status": "exception",
                "error": str(e)[:200],
                "elapsed": round(elapsed, 1),
                "failureReason": classification.get("failureReason", "unknown"),
                "recommendedStrategy": classification.get("recommendedStrategy", "diagnose_with_msrp_page_analyzer"),
                "severity": classification.get("severity", "warning"),
            })
            error_count += 1
    # Summary
    total = len(target_codes)
    print(f"\n{'='*70}")
    print(f"Results: {pass_count}/{total} PASS, {empty_count} empty, "
          f"{fail_count} failures, {error_count} errors")
    print(f"{'='*70}")

    # By-country summary
    from collections import Counter
    by_country = {}
    for r in results:
        cc = r["country"]
        by_country.setdefault(cc, Counter())
        if _is_passing_result(r):
            by_country[cc]["pass"] += 1
        elif r["status"] == "empty":
            by_country[cc]["empty"] += 1
        else:
            by_country[cc]["fail"] += 1

    print(
        f"\n{'Country':8s} {'Pass':>6s} {'Empty':>6s} "
        f"{'Fail':>6s} {'Total':>6s}"
    )
    for cc in sorted(by_country):
        c = by_country[cc]
        t = c["pass"] + c["empty"] + c["fail"]
        print(f"{cc:8s} {c['pass']:6d} {c['empty']:6d} {c['fail']:6d} {t:6d}")

    # Build failure breakdown and strategy recommendations
    failure_breakdown: dict[str, int] = {}
    strategy_recs: dict[str, int] = {}
    for r in results:
        reason = r.get("failureReason")
        if reason:
            failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
        strat = r.get("recommendedStrategy")
        if strat:
            strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

    _write_dryrun_status_best_effort(
        countries,
        pass_count,
        empty_count,
        fail_count,
        error_count,
        total=total,
        results=results,
    )

    if _is_child_run():
        if STRICT_EXIT and (fail_count > 0 or error_count > 0):
            raise SystemExit(1)
        return

    # Save v3 report (timestamped history + latest pointer)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_id = f"msrp-dryrun-{ts}"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    report_dir = Path(__file__).parent / "diagnostics" / "artifacts"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_payload = _build_dryrun_report_payload(
        batch=batch,
        countries=countries,
        results=results,
        run_id=run_id,
        generated_at=generated_at,
    )

    # Timestamped copy for history
    history_path = report_dir / f"dryrun_report_{run_id}.json"
    with open(history_path, "w") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    # Also overwrite latest for backward compat
    latest_path = report_dir / "dryrun_report.json"
    with open(latest_path, "w") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    _write_dryrun_runs_index(report_payload, latest_path, history_path)
    _write_source_repair_backlog_artifact(report_payload, report_dir)

    print(f"\nReport saved to {latest_path} (history: {history_path.name})")

    if STRICT_EXIT and (fail_count > 0 or error_count > 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
