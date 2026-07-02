"""Standalone scrape runner — orchestrates extract, validate, and submit."""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from jato_scraper import registry
from jato_scraper.base import BaseExtractor, RawObservation
from jato_scraper.config_loader import load_all_sources, load_source_file
from jato_scraper.currency_converter import enrich_observations_with_eur
from jato_scraper.msrp_batch_config import resolve_msrp_batch_source_refs
from jato_scraper.validation import (
    BatchValidationReport,
    validate_observations,
)

log = logging.getLogger(__name__)
DEFAULT_API_BASE = "http://localhost:8000/v1"
SOURCE_FILE_SUFFIXES = frozenset({".yaml", ".yml"})
DEFAULT_SOURCE_TIMEOUT_SECONDS = 180
FINANCE_CONTEXT_FIELDS = frozenset({
    "price_semantics",
    "monthly_payment",
    "down_payment",
    "down_payment_pct",
    "term_months",
    "apr",
    "effective_apr",
    "balloon_payment",
    "finance_type",
    "total_credit_cost",
    "total_amount_payable",
    "annual_mileage_limit",
    "offer_valid_until",
    "subsidy_amount",
    "net_price_after_subsidy",
    "finance_currency",
})
WRITE_ROLE_LEVELS = {
    "viewer": 1,
    "order_filler": 1,
    "editor": 2,
    "admin": 3,
    "developer": 3,
}
MIN_WRITE_ROLE_LEVEL = WRITE_ROLE_LEVELS["editor"]


class SourceTimeoutError(TimeoutError):
    """Raised when one source extraction exceeds its per-source budget."""


def _default_source_timeout_seconds() -> int:
    raw = os.getenv("JATO_SOURCE_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return DEFAULT_SOURCE_TIMEOUT_SECONDS
    try:
        return max(0, int(raw))
    except ValueError:
        log.warning(
            "Invalid JATO_SOURCE_TIMEOUT_SECONDS=%r; using default %s",
            raw,
            DEFAULT_SOURCE_TIMEOUT_SECONDS,
        )
        return DEFAULT_SOURCE_TIMEOUT_SECONDS


def _source_diagnostics_from_extractor(
    extractor: BaseExtractor,
) -> dict[str, Any]:
    cfg = extractor.config
    diagnostics: dict[str, Any] = {
        "sourceUrl": getattr(cfg, "source_url", None),
        "brand": getattr(cfg, "brand", None),
        "extractorName": getattr(
            extractor,
            "extractor_name",
            type(extractor).__name__,
        ),
        "extractorVersion": getattr(extractor, "extractor_version", None),
    }
    audit_event = getattr(extractor, "last_audit_event", None)
    if isinstance(audit_event, dict):
        diagnostics.update({
            "auditStatus": audit_event.get("status"),
            "attemptedStrategies": audit_event.get("attempted_strategies"),
            "winningStrategy": audit_event.get("winning_strategy"),
            "coverageLevel": audit_event.get("coverage_level"),
        })
        if audit_event.get("error"):
            diagnostics["extractorError"] = audit_event["error"]
        if audit_event.get("httpStatus") is not None:
            diagnostics["httpStatus"] = audit_event["httpStatus"]
        if audit_event.get("finalUrl"):
            diagnostics["finalUrl"] = audit_event["finalUrl"]
        if audit_event.get("contentType"):
            diagnostics["contentType"] = audit_event["contentType"]

    return {
        key: value
        for key, value in diagnostics.items()
        if value not in ("", [], {})
    }


def _extract_in_child(
    extractor: BaseExtractor,
    output_queue: multiprocessing.Queue,
) -> None:
    try:
        observations = extractor.extract()
        output_queue.put((
            "ok",
            observations,
            _source_diagnostics_from_extractor(extractor),
        ))
    except BaseException as exc:
        output_queue.put(("error", type(exc).__name__, str(exc)))


def _extract_with_timeout(
    extractor: BaseExtractor,
    *,
    source_code: str,
    timeout_seconds: int,
) -> tuple[list[RawObservation], dict[str, Any]]:
    if timeout_seconds <= 0:
        observations = extractor.extract()
        return observations, _source_diagnostics_from_extractor(extractor)
    if "fork" not in multiprocessing.get_all_start_methods():
        log.warning(
            "Per-source hard timeout requires fork; running %s inline",
            source_code,
        )
        observations = extractor.extract()
        return observations, _source_diagnostics_from_extractor(extractor)

    ctx = multiprocessing.get_context("fork")
    output_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_extract_in_child,
        args=(extractor, output_queue),
        daemon=True,
    )
    process.start()
    process.join(timeout_seconds)

    try:
        if process.is_alive():
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            raise SourceTimeoutError(
                f"source {source_code} exceeded "
                f"{timeout_seconds}s extraction timeout"
            )

        try:
            status, payload, *extra = output_queue.get_nowait()
        except queue.Empty as exc:
            raise RuntimeError(
                f"source {source_code} extractor process exited with "
                f"code {process.exitcode} without a result"
            ) from exc
        if status == "ok":
            diagnostics = extra[0] if extra and isinstance(extra[0], dict) else {}
            return payload, diagnostics
        exc_type = str(payload)
        message = str(extra[0]) if extra else ""
        raise RuntimeError(
            f"{exc_type}: {message}".strip(": ")
        )
    finally:
        output_queue.close()
        output_queue.join_thread()


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _normalize_country_filter(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    normalized = {value.strip().upper() for value in values if value.strip()}
    return normalized or None


def _expand_source_ref(source_ref: str) -> list[str]:
    candidate = Path(source_ref).expanduser()
    is_path_like = (
        candidate.suffix.lower() in SOURCE_FILE_SUFFIXES
        or candidate.exists()
    )
    if not is_path_like:
        return [source_ref]

    resolved = candidate.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Source path does not exist: {source_ref}")

    if resolved.is_dir():
        return load_all_sources(sources_dir=resolved)

    if resolved.suffix.lower() not in SOURCE_FILE_SUFFIXES:
        raise ValueError(f"Unsupported source file type: {source_ref}")

    source_code = load_source_file(resolved)
    if not source_code:
        raise ValueError(f"Failed to load source file: {source_ref}")
    return [source_code]


def _resolve_source_codes(source_refs: list[str]) -> list[str]:
    load_all_sources()
    resolved_codes: list[str] = []
    for source_ref in source_refs:
        resolved_codes.extend(_expand_source_ref(source_ref))
    return _dedupe_preserve_order(resolved_codes)


def _auth_headers(
    auth_token: str | None = None,
    user_name: str | None = None,
) -> dict[str, str]:
    token = (
        auth_token
        or os.getenv("JATO_AUTH_TOKEN")
        or os.getenv("APP_AUTH_TOKEN")
        or os.getenv("VITE_AUTH_TOKEN")
        or "change-me"
    ).strip()
    user = (
        user_name
        or os.getenv("JATO_USER_NAME")
        or os.getenv("APP_USER_NAME")
        or "copilot"
    ).strip() or "copilot"
    return {
        "X-Auth-Token": token,
        "X-User-Name": user,
    }


def _verify_write_auth(
    api_base: str,
    auth_token: str | None = None,
    user_name: str | None = None,
) -> dict[str, Any]:
    url = f"{api_base}/auth/me"
    try:
        resp = requests.get(
            url,
            headers=_auth_headers(auth_token, user_name),
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        return {
            "ok": False,
            "status": "auth_failed",
            "reason": "auth_preflight_request_failed",
            "error": str(exc),
        }
    except ValueError as exc:
        return {
            "ok": False,
            "status": "auth_failed",
            "reason": "auth_preflight_invalid_json",
            "error": str(exc),
        }

    role = str(payload.get("role") or "").strip().lower()
    role_level = WRITE_ROLE_LEVELS.get(role, 0)
    if role_level < MIN_WRITE_ROLE_LEVEL:
        return {
            "ok": False,
            "status": "auth_failed",
            "reason": "write_role_required",
            "role": role or "unknown",
            "requiredRole": "editor",
        }
    return {
        "ok": True,
        "status": "ok",
        "role": role,
        "user": payload.get("username") or payload.get("name"),
    }


def _observation_to_ingest_dict(
    obs: RawObservation,
    source_id: str,
    extractor: BaseExtractor,
) -> dict[str, Any]:
    # Map scraper match_status to backend enum values
    _STATUS_MAP = {
        "auto_accept": "auto_accepted",
        "force_review": "review_required",
        "review": "review_required",
    }
    return {
        "source_id": source_id,
        "country": extractor.config.country,
        "brand": extractor.config.brand,
        "jato_model": obs.jato_model,
        "jato_trim": obs.jato_trim,
        "jato_powertrain": obs.jato_powertrain,
        "official_model": obs.official_model,
        "official_trim": obs.official_trim,
        "official_edition": obs.official_edition,
        "official_powertrain": obs.official_powertrain,
        "msrp_value": obs.msrp_value,
        "currency": obs.currency,
        "tax_included": obs.tax_included,
        "price_label": obs.price_label or "MSRP",
        "availability_text": obs.availability_text,
        "observed_at_utc": obs.observed_at_utc.isoformat(),
        "source_url": obs.source_url,
        "source_payload_hash": obs.payload_hash,
        "extraction_version": extractor.extractor_version,
        "match_confidence": obs.match_confidence,
        "match_status": _STATUS_MAP.get(obs.match_status, obs.match_status),
        "match_reason_json": obs.match_reason,
        "candidate_matches_json": obs.candidate_matches,
        "msrp_value_eur": obs.msrp_value_eur,
        "fx_rate_to_eur": obs.fx_rate_to_eur,
        "source_context_json": _source_context_from_raw_payload(
            obs.raw_payload,
        ),
        "price_semantics": (
            obs.raw_payload.get("price_semantics")
            if obs.raw_payload
            else None
        ),
    }


def _source_context_from_raw_payload(
    raw_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if not raw_payload:
        return None

    source_context: dict[str, Any] = {"rawPayload": raw_payload}
    pricing_context = raw_payload.get("pricingContext")
    if isinstance(pricing_context, dict):
        source_context["pricingContext"] = pricing_context
        return source_context

    pricing_context = {
        key: raw_payload[key]
        for key in FINANCE_CONTEXT_FIELDS
        if raw_payload.get(key) is not None
    }
    if pricing_context:
        source_context["pricingContext"] = pricing_context

    return source_context


def _pricing_context_from_raw_payload(
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    if not raw_payload:
        return {}
    pricing_context = raw_payload.get("pricingContext")
    if isinstance(pricing_context, dict):
        return {
            str(key): value
            for key, value in pricing_context.items()
            if value is not None
        }
    return {
        key: raw_payload[key]
        for key in FINANCE_CONTEXT_FIELDS
        if raw_payload.get(key) is not None
    }


def _increment_counter(
    counts: dict[str, int],
    value: object,
) -> None:
    label = str(value or "").strip() or "unknown"
    counts[label] = counts.get(label, 0) + 1


def _finance_summary_from_observations(
    observations: list[RawObservation],
) -> dict[str, Any]:
    candidates = 0
    monthly_count = 0
    semantics_counts: dict[str, int] = {}
    finance_type_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []

    for observation in observations:
        context = _pricing_context_from_raw_payload(observation.raw_payload)
        if not context:
            continue
        semantics = context.get("price_semantics")
        has_finance_signal = bool(
            semantics
            or context.get("finance_type")
            or context.get("monthly_payment") is not None
            or context.get("down_payment") is not None
            or context.get("subsidy_amount") is not None
            or context.get("net_price_after_subsidy") is not None
        )
        if not has_finance_signal:
            continue
        candidates += 1
        if context.get("monthly_payment") is not None:
            monthly_count += 1
        _increment_counter(semantics_counts, semantics)
        _increment_counter(finance_type_counts, context.get("finance_type"))
        if len(samples) < 5:
            samples.append({
                "officialModel": observation.official_model,
                "officialTrim": observation.official_trim,
                "jatoModel": observation.jato_model,
                "jatoTrim": observation.jato_trim,
                "priceSemantics": semantics,
                "financeType": context.get("finance_type"),
                "monthlyPayment": context.get("monthly_payment"),
                "financeCurrency": context.get("finance_currency")
                or observation.currency,
            })

    return {
        "financeObservationCandidates": candidates,
        "financeMonthlyPaymentCount": monthly_count,
        "financeSemanticsCounts": semantics_counts,
        "financeTypeCounts": finance_type_counts,
        "sampleFinanceContexts": samples,
    }


def _rejection_diagnostics_from_report(
    report: BatchValidationReport,
    sample_limit: int = 5,
) -> dict[str, Any]:
    if not report.rejected:
        return {}

    reason_counts: dict[str, int] = {}
    rule_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []

    for observation, failures in report.rejected:
        reasons = [failure.reason for failure in failures]
        rules = [failure.rule for failure in failures]
        for reason in reasons:
            _increment_counter(reason_counts, reason)
        for rule in rules:
            _increment_counter(rule_counts, rule)
        if len(samples) >= sample_limit:
            continue
        raw_payload = (
            observation.raw_payload
            if isinstance(observation.raw_payload, dict)
            else {}
        )
        sample: dict[str, Any] = {
            "officialModel": observation.official_model,
            "officialTrim": observation.official_trim,
            "msrpValue": observation.msrp_value,
            "currency": observation.currency,
            "priceLabel": observation.price_label,
            "reasons": reasons,
            "rules": rules,
        }
        price_text = raw_payload.get("priceText")
        if price_text not in (None, ""):
            sample["priceText"] = price_text
        pricing_context = raw_payload.get("pricingContext")
        if isinstance(pricing_context, dict) and pricing_context:
            sample["pricingContext"] = pricing_context
        samples.append(sample)

    return {
        "rejectedReasons": sorted(reason_counts),
        "rejectedRules": sorted(rule_counts),
        "rejectionReasonCounts": dict(sorted(reason_counts.items())),
        "rejectionRuleCounts": dict(sorted(rule_counts.items())),
        "sampleRejectedObservations": samples,
    }


def build_batch_payload(
    extractor: BaseExtractor,
    report: BatchValidationReport,
    source_id: str,
    trigger_type: str = "scheduled",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "batch_code": (
            f"{extractor.config.source_code}_{now:%Y%m%d_%H%M%S}_"
            f"{uuid.uuid4().hex[:6]}"
        ),
        "trigger_type": trigger_type,
        "scope_country": extractor.config.country,
        "scope_brands": [extractor.config.brand],
        "failed_count": len(report.rejected),
        "notes": None,
        "started_at_utc": now.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "observations": [
            _observation_to_ingest_dict(obs, source_id, extractor)
            for obs in report.valid
        ],
    }


def submit_batch(
    payload: dict[str, Any],
    api_base: str,
    auth_token: str | None = None,
    user_name: str | None = None,
) -> dict:
    url = f"{api_base}/msrp/batches"
    resp = requests.post(
        url,
        json=payload,
        headers=_auth_headers(auth_token, user_name),
        timeout=60,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        response_body = (getattr(resp, "text", "") or "").strip()
        if len(response_body) > 4000:
            response_body = f"{response_body[:4000]}..."
        if response_body:
            raise requests.HTTPError(
                f"{exc}; response_body={response_body}",
                response=resp,
            ) from exc
        raise
    return resp.json()


def _build_source_patch_payload(
    existing_source: dict[str, Any],
    expected_source: dict[str, Any],
) -> dict[str, Any]:
    field_map = {
        "brand": "brand",
        "source_url": "sourceUrl",
        "source_type": "sourceType",
        "extractor_name": "extractorName",
        "extractor_version": "extractorVersion",
        "price_semantics": "priceSemantics",
        "requires_location": "requiresLocation",
        "enabled": "enabled",
        "notes": "notes",
    }
    patch_payload: dict[str, Any] = {}
    for expected_key, existing_key in field_map.items():
        expected_value = expected_source.get(expected_key)
        existing_value = existing_source.get(existing_key)
        if existing_value != expected_value:
            patch_payload[expected_key] = expected_value
    return patch_payload


def ensure_source(
    extractor: BaseExtractor,
    api_base: str,
    auth_token: str | None = None,
    user_name: str | None = None,
) -> str:
    code = extractor.config.source_code
    url = f"{api_base}/msrp/sources"
    resp = requests.get(
        url,
        params={"source_code": code},
        headers=_auth_headers(auth_token, user_name),
        timeout=30,
    )
    if resp.ok:
        payload = resp.json()
        items = []
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("items"), list):
                items = payload["items"]
            else:
                items = [payload]
        for source in items:
            if (
                source.get("source_code") == code
                or source.get("sourceCode") == code
            ):
                source_id = source.get("source_id") or source.get("sourceId")
                if source_id:
                    expected_payload = extractor.to_source_payload()
                    patch_payload = _build_source_patch_payload(
                        source,
                        expected_payload,
                    )
                    if patch_payload:
                        patch_resp = requests.patch(
                            f"{url}/{source_id}",
                            json=patch_payload,
                            headers=_auth_headers(auth_token, user_name),
                            timeout=30,
                        )
                        patch_resp.raise_for_status()
                return source_id
    payload = extractor.to_source_payload()
    resp = requests.post(
        url,
        json=payload,
        headers=_auth_headers(auth_token, user_name),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("source_id") or data.get("sourceId")


def run_scrape(
    source_codes: list[str],
    api_base: str = DEFAULT_API_BASE,
    trigger_type: str = "scheduled",
    dry_run: bool = False,
    auth_token: str | None = None,
    user_name: str | None = None,
    source_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    timeout_seconds = (
        _default_source_timeout_seconds()
        if source_timeout_seconds is None
        else max(0, int(source_timeout_seconds))
    )
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    summary: dict[str, Any] = {"sources": {}, "ok": True, "run_id": run_id}
    if not dry_run:
        auth_status = _verify_write_auth(
            api_base,
            auth_token=auth_token,
            user_name=user_name,
        )
        summary["auth"] = auth_status
        if not auth_status.get("ok"):
            log.error(
                "Write auth preflight failed for %s: %s",
                api_base,
                auth_status,
            )
            summary["ok"] = False
            return summary

    resolved_codes = _resolve_source_codes(source_codes)
    for code in resolved_codes:
        log.info("── Scraping %s ──", code)
        source_result: dict[str, Any] = {"status": "error"}
        try:
            extractor = registry.get(code)
            extractor.run_id = run_id
            source_result.update(_source_diagnostics_from_extractor(extractor))
        except KeyError as exc:
            log.error("%s", exc)
            source_result["error"] = str(exc)
            summary["sources"][code] = source_result
            summary["ok"] = False
            continue
        try:
            observations, diagnostics = _extract_with_timeout(
                extractor,
                source_code=code,
                timeout_seconds=timeout_seconds,
            )
            source_result.update(diagnostics)
        except SourceTimeoutError as exc:
            error = str(exc)
            log.error("Extraction timed out for %s: %s", code, error)
            extractor.record_strategy_audit(
                url=extractor.config.source_url,
                strategy="extractor",
                observations=[],
                winning_strategy=None,
                error=error,
            )
            source_result.update(_source_diagnostics_from_extractor(extractor))
            source_result.update(
                status="timeout",
                extracted=0,
                error=error,
                sourceTimeoutSeconds=timeout_seconds,
            )
            summary["sources"][code] = source_result
            summary["ok"] = False
            continue
        except Exception as exc:
            error = str(exc)
            log.exception("Extraction failed for %s: %s", code, error)
            extractor.record_strategy_audit(
                url=extractor.config.source_url,
                strategy="extractor",
                observations=[],
                winning_strategy=None,
                error=error,
            )
            source_result.update(_source_diagnostics_from_extractor(extractor))
            source_result.update(
                status="error",
                extracted=0,
                error=error,
            )
            summary["sources"][code] = source_result
            summary["ok"] = False
            continue
        log.info("Got %d raw observations", len(observations))
        if not observations:
            source_result.update(status="empty", extracted=0)
            summary["sources"][code] = source_result
            continue
        report = validate_observations(
            observations,
            country=extractor.config.country,
            source_price_semantics=getattr(
                extractor.config,
                "price_semantics",
                "base_msrp",
            ),
        )
        # --- EUR conversion ---
        enrich_observations_with_eur(report.valid)
        for obs in report.valid:
            if obs.msrp_value_eur is not None:
                log.info(
                    "  FX %s %s → %.2f EUR (rate=%.4f)",
                    f"{obs.msrp_value:,.0f}",
                    obs.currency,
                    obs.msrp_value_eur,
                    obs.fx_rate_to_eur or 0,
                )
        log.info(
            "Validation: %d valid, %d rejected (of %d total)",
            len(report.valid),
            len(report.rejected),
            report.total,
        )
        for obs, failures in report.rejected:
            reasons = "; ".join(f.reason for f in failures)
            log.warning(
                "  REJECTED %s / %s — %s",
                obs.official_model,
                obs.official_trim,
                reasons,
            )
        source_result.update(
            extracted=len(observations),
            valid=len(report.valid),
            rejected=len(report.rejected),
            **_finance_summary_from_observations(report.valid),
            **_rejection_diagnostics_from_report(report),
        )
        if dry_run:
            source_result["status"] = "dry_run"
            summary["sources"][code] = source_result
            continue
        if not report.valid:
            source_result["status"] = "all_rejected"
            summary["sources"][code] = source_result
            continue
        try:
            source_id = ensure_source(
                extractor,
                api_base,
                auth_token=auth_token,
                user_name=user_name,
            )
        except requests.RequestException as exc:
            log.error("Failed to ensure source %s: %s", code, exc)
            source_result["error"] = f"ensure_source failed: {exc}"
            summary["sources"][code] = source_result
            summary["ok"] = False
            continue
        payload = build_batch_payload(
            extractor,
            report,
            source_id,
            trigger_type,
        )
        try:
            result = submit_batch(
                payload,
                api_base,
                auth_token=auth_token,
                user_name=user_name,
            )
            source_result.update(status="ok", api_response=result)
        except requests.RequestException as exc:
            log.error("Failed to submit batch for %s: %s", code, exc)
            source_result["error"] = f"submit_batch failed: {exc}"
            summary["ok"] = False
        summary["sources"][code] = source_result
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="JATO MSRP scraping toolkit")
    parser.add_argument("--sources", nargs="+", metavar="REF")
    parser.add_argument("--batch-files", nargs="+", metavar="BATCH")
    parser.add_argument(
        "--countries",
        nargs="*",
        metavar="CC",
        help="Optional country codes to keep when using --batch-files.",
    )
    parser.add_argument("--all", action="store_true", dest="scrape_all")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument(
        "--trigger",
        default="scheduled",
        choices=["manual", "scheduled"],
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--source-timeout-seconds",
        type=int,
        default=None,
        help=(
            "Maximum extraction time per source; defaults to "
            "JATO_SOURCE_TIMEOUT_SECONDS or 180. Use 0 to disable."
        ),
    )
    parser.add_argument("--auth-token", default=None)
    parser.add_argument("--user-name", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    selected_mode_count = sum(
        bool(value)
        for value in (args.scrape_all, args.sources, args.batch_files)
    )
    if selected_mode_count != 1:
        parser.error(
            "Specify exactly one of --sources REF [REF ...], "
            "--batch-files BATCH [BATCH ...], or --all",
        )
        return
    if args.countries and not args.batch_files:
        parser.error("--countries requires --batch-files")
        return
    if args.scrape_all:
        load_all_sources()
        source_refs = registry.list_registered()
    elif args.batch_files:
        source_refs = resolve_msrp_batch_source_refs(
            args.batch_files,
            country_filter=_normalize_country_filter(args.countries),
        )
    elif args.sources:
        source_refs = args.sources
    else:
        parser.error("Specify --sources REF [REF ...], --batch-files, or --all")
        return
    if not source_refs:
        log.warning("No extractors registered — nothing to scrape.")
        sys.exit(0)
    try:
        summary = run_scrape(
            source_codes=source_refs,
            api_base=args.api_base,
            trigger_type=args.trigger,
            dry_run=args.dry_run,
            auth_token=args.auth_token,
            user_name=args.user_name,
            source_timeout_seconds=args.source_timeout_seconds,
        )
    except (FileNotFoundError, ValueError) as exc:
        parser.error(str(exc))
        return
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    sys.exit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
