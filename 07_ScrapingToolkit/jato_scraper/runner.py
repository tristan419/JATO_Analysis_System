"""Standalone scrape runner — orchestrates extract, validate, and submit."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from jato_scraper import registry
from jato_scraper.base import BaseExtractor, RawObservation
from jato_scraper.config_loader import load_all_sources
from jato_scraper.currency_converter import enrich_observations_with_eur
from jato_scraper.validation import (
    BatchValidationReport,
    validate_observations,
)

log = logging.getLogger(__name__)
DEFAULT_API_BASE = "http://localhost:8000/v1"


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
    resp.raise_for_status()
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
) -> dict[str, Any]:
    # Auto-load YAML source configs before running
    load_all_sources()
    summary: dict[str, Any] = {"sources": {}, "ok": True}
    for code in source_codes:
        log.info("── Scraping %s ──", code)
        source_result: dict[str, Any] = {"status": "error"}
        try:
            extractor = registry.get(code)
        except KeyError as exc:
            log.error("%s", exc)
            source_result["error"] = str(exc)
            summary["sources"][code] = source_result
            summary["ok"] = False
            continue
        observations = extractor.extract()
        log.info("Got %d raw observations", len(observations))
        if not observations:
            source_result.update(status="empty", extracted=0)
            summary["sources"][code] = source_result
            continue
        report = validate_observations(
            observations,
            country=extractor.config.country,
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
    parser.add_argument("--sources", nargs="+", metavar="CODE")
    parser.add_argument("--all", action="store_true", dest="scrape_all")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument(
        "--trigger",
        default="scheduled",
        choices=["manual", "scheduled"],
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--auth-token", default=None)
    parser.add_argument("--user-name", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    if args.scrape_all:
        load_all_sources()
        codes = registry.list_registered()
    elif args.sources:
        codes = args.sources
    else:
        parser.error("Specify --sources CODE [CODE ...] or --all")
        return
    if not codes:
        log.warning("No extractors registered — nothing to scrape.")
        sys.exit(0)
    summary = run_scrape(
        source_codes=codes,
        api_base=args.api_base,
        trigger_type=args.trigger,
        dry_run=args.dry_run,
        auth_token=args.auth_token,
        user_name=args.user_name,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    sys.exit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
