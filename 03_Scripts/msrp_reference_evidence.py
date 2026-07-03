#!/usr/bin/env python3
"""Build third-party reference evidence for MSRP source repair.

The output is intentionally review-only. It must not be counted as an
official MSRP dryrun pass or used as an ingest source for current_prices.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLKIT_ROOT = REPO_ROOT / "07_ScrapingToolkit"
if str(TOOLKIT_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLKIT_ROOT))

from jato_scraper.evkx_catalog import (  # noqa: E402
    DEFAULT_HEADERS,
    EvkxFetchOptions,
    absolute_evkx_url,
    fetch_search_catalog,
    select_local_pricing_items,
)

DEFAULT_BACKLOG_PATH = (
    REPO_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_source_repair_backlog.json"
)

COUNTRY_NAME_BY_CODE = {
    "at": "Austria",
    "be": "Belgium",
    "ch": "Switzerland",
    "cz": "CzechRepublic",
    "de": "Germany",
    "dk": "Denmark",
    "es": "Spain",
    "fi": "Finland",
    "fr": "France",
    "hr": "Croatia",
    "hu": "Hungary",
    "it": "Italy",
    "nl": "Netherlands",
    "no": "Norway",
    "pl": "Poland",
    "pt": "Portugal",
    "se": "Sweden",
    "uk": "UnitedKingdom",
    "us": "UnitedStates",
}

TESLA_EVKX_REFERENCE_ASSIST = {
    "preferred": "official_proxy_or_configurator_api",
    "thirdPartyReference": "EVKX",
    "referencePolicy": "reference_only_review_required",
    "officialSourceRequiredForIngest": True,
    "acceptanceRules": [
        "Do not count EVKX as an official MSRP dryrun pass.",
        "Only use EVKX records when pricingCountry matches the target country.",
        "Only use EVKX records when isConverted is false.",
        "Keep the Tesla official source open until an official page, price list, or configurator API is fetchable.",
    ],
    "reason": (
        "Tesla official pages are blocked by the direct fetch path; EVKX can "
        "supply BEV variant and local-price reference evidence but cannot "
        "replace official MSRP evidence."
    ),
}

SOURCE_SUFFIXES = (
    "_draft_scrapling",
    "_draft_playwright",
    "_draft_http_json",
    "_draft_pdf_text",
    "_scrapling",
    "_playwright",
    "_http_json",
    "_pdf_text",
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return data


def _country_from_source_code(source_code: str) -> str:
    normalized = source_code.strip().lower()
    for suffix in SOURCE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized.removesuffix(suffix)
            break
    parts = [part for part in normalized.split("_") if part]
    return parts[-1] if parts and len(parts[-1]) == 2 else ""


def _brand_label(brand: str) -> str:
    words = [part for part in re.split(r"[^A-Za-z0-9]+", brand.strip()) if part]
    return " ".join(word.capitalize() for word in words)


def _source_model_query(source_code: str, *, brand: str, country_code: str) -> str:
    normalized = source_code.strip().lower()
    for suffix in SOURCE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized.removesuffix(suffix)
            break
    country = country_code.strip().lower()
    if country and normalized.endswith(f"_{country}"):
        normalized = normalized[: -(len(country) + 1)]
    brand_slug = re.sub(r"[^a-z0-9]+", "_", brand.strip().lower()).strip("_")
    if brand_slug and normalized.startswith(f"{brand_slug}_"):
        normalized = normalized[len(brand_slug) + 1 :]
    model = " ".join(part.capitalize() for part in normalized.split("_") if part)
    brand_text = _brand_label(brand)
    return " ".join(part for part in (brand_text, model) if part).strip()


def _reference_groups(backlog: dict[str, Any]) -> list[dict[str, Any]]:
    groups = backlog.get("groups") or []
    if not isinstance(groups, list):
        return []
    return [
        group
        for group in groups
        if isinstance(group, dict)
        and (group.get("referenceAssist") or {}).get("thirdPartyReference") == "EVKX"
    ]


def _is_tesla_source(source: dict[str, Any]) -> bool:
    brand = str(source.get("brand") or "").strip().upper()
    host = str(source.get("host") or "").strip().lower()
    source_code = str(source.get("sourceCode") or source.get("code") or "").lower()
    return brand == "TESLA" or host == "tesla.com" or source_code.startswith("tesla_")


def _iter_source_repair_issues(backlog: dict[str, Any]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []
    candidates: list[Any] = []
    candidates.extend(backlog.get("sourceIssues") or [])
    candidates.extend(backlog.get("externalAccessIssues") or [])
    for group in backlog.get("groups") or []:
        if isinstance(group, dict):
            candidates.extend(group.get("sourceRepairIssues") or [])
            candidates.extend(group.get("externalAccessIssues") or [])
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        source_code = str(candidate.get("sourceCode") or candidate.get("code") or "").strip()
        country_code = str(
            candidate.get("countryCode")
            or candidate.get("country")
            or _country_from_source_code(source_code)
        ).strip().lower()
        key = (country_code, source_code)
        if not country_code or not source_code or key in seen:
            continue
        seen.add(key)
        items.append(candidate)
    return items


def _work_items_from_backlog(backlog: dict[str, Any]) -> list[dict[str, Any]]:
    items: dict[tuple[str, str, str], dict[str, Any]] = {}
    for group in _reference_groups(backlog):
        brands = [
            str(brand).strip().upper()
            for brand in group.get("affectedBrands") or []
            if str(brand).strip()
        ]
        brand = brands[0] if len(brands) == 1 else ""
        sources = [
            str(source).strip()
            for source in group.get("sampleSources") or []
            if str(source).strip()
        ]
        for source_code in sources:
            country_code = _country_from_source_code(source_code)
            if not country_code:
                continue
            model_query = _source_model_query(
                source_code,
                brand=brand,
                country_code=country_code,
            )
            if not model_query:
                continue
            key = (country_code, brand, model_query)
            entry = items.setdefault(
                key,
                {
                    "countryCode": country_code,
                    "pricingCountry": COUNTRY_NAME_BY_CODE.get(country_code, ""),
                    "brand": brand,
                    "modelQuery": model_query,
                    "sourceCodes": [],
                    "failureReason": group.get("failureReason"),
                    "recommendedStrategy": group.get("recommendedStrategy"),
                    "referenceAssist": group.get("referenceAssist"),
                },
            )
            entry["sourceCodes"].append(source_code)
    for source in _iter_source_repair_issues(backlog):
        if not _is_tesla_source(source):
            continue
        source_code = str(source.get("sourceCode") or source.get("code") or "").strip()
        country_code = str(
            source.get("countryCode")
            or source.get("country")
            or _country_from_source_code(source_code)
        ).strip().lower()
        brand = "TESLA"
        model_query = _source_model_query(
            source_code,
            brand=brand,
            country_code=country_code,
        )
        if not country_code or not model_query:
            continue
        key = (country_code, brand, model_query)
        entry = items.setdefault(
            key,
            {
                "countryCode": country_code,
                "pricingCountry": COUNTRY_NAME_BY_CODE.get(country_code, ""),
                "brand": brand,
                "modelQuery": model_query,
                "sourceCodes": [],
                "failureReason": source.get("failureReason"),
                "recommendedStrategy": source.get("recommendedStrategy"),
                "referenceAssist": TESLA_EVKX_REFERENCE_ASSIST,
            },
        )
        if source_code not in entry["sourceCodes"]:
            entry["sourceCodes"].append(source_code)
    return sorted(
        items.values(),
        key=lambda item: (
            str(item["countryCode"]),
            str(item["brand"]),
            str(item["modelQuery"]),
        ),
    )


def _evkx_reference(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "evId": item.get("evId"),
        "name": item.get("name"),
        "startPrice": item.get("startPrice"),
        "currency": item.get("currency"),
        "pricingCountry": item.get("pricingCountry"),
        "isConverted": item.get("isConverted"),
        "infoUrl": absolute_evkx_url(str(item.get("infoUri") or "")),
    }


def build_reference_evidence(
    backlog: dict[str, Any],
    *,
    session: requests.Session,
    page_size: int = 1000,
    max_pages: int | None = 2,
) -> dict[str, Any]:
    evidence_items: list[dict[str, Any]] = []
    for work_item in _work_items_from_backlog(backlog):
        pricing_country = str(work_item.get("pricingCountry") or "")
        model_query = str(work_item.get("modelQuery") or "")
        references: list[dict[str, Any]] = []
        fetch_error: str | None = None
        if not pricing_country:
            fetch_error = "unsupported_country_for_evkx_pricing"
        else:
            try:
                catalog_items = fetch_search_catalog(
                    session,
                    EvkxFetchOptions(
                        pricing_country=pricing_country,
                        availability_filter="current",
                        page_size=page_size,
                        max_pages=max_pages,
                        include_details=False,
                    ),
                )
                references = [
                    _evkx_reference(item)
                    for item in select_local_pricing_items(
                        catalog_items,
                        pricing_country=pricing_country,
                        name_contains=model_query,
                    )
                ]
            except Exception as exc:  # pragma: no cover - covered by live runs.
                fetch_error = f"{type(exc).__name__}: {exc}"

        evidence_items.append({
            **work_item,
            "referenceSource": "EVKX",
            "referencePolicy": "reference_only_review_required",
            "officialIngestEligible": False,
            "localReferenceCount": len(references),
            "localPriceReferences": references,
            "fetchError": fetch_error,
            "reviewRecommendation": (
                "use_as_review_reference_only"
                if references
                else "continue_official_source_repair"
            ),
        })

    local_reference_count = sum(
        int(item["localReferenceCount"]) for item in evidence_items
    )
    missing_reference_count = sum(
        1 for item in evidence_items if int(item["localReferenceCount"]) == 0
    )
    return {
        "schemaVersion": "msrp_source_reference_evidence_v1",
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "backlogRunId": backlog.get("runId"),
        "referenceSource": "EVKX",
        "referencePolicy": "reference_only_review_required",
        "officialSourceRequiredForIngest": True,
        "officialIngestEligible": False,
        "summary": {
            "evidenceItemCount": len(evidence_items),
            "localReferenceCount": local_reference_count,
            "missingLocalReferenceCount": missing_reference_count,
            "officialIngestEligibleCount": 0,
        },
        "items": evidence_items,
    }


def _write_markdown(payload: dict[str, Any], path: Path) -> None:
    lines = [
        "# MSRP Source Reference Evidence",
        "",
        f"Generated: {payload['generatedAt']}",
        f"Backlog run: {payload.get('backlogRunId') or '-'}",
        "Policy: EVKX reference only, official source still required for ingest",
        "",
        "| Country | Model query | References | Recommendation |",
        "|---|---|---:|---|",
    ]
    for item in payload.get("items") or []:
        lines.append(
            "| {country} | {model} | {count} | {recommendation} |".format(
                country=str(item.get("countryCode") or "").upper(),
                model=item.get("modelQuery") or "-",
                count=item.get("localReferenceCount") or 0,
                recommendation=item.get("reviewRecommendation") or "-",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    backlog_path: str | None = None,
    out_dir: str | None = None,
    *,
    page_size: int = 1000,
    max_pages: int | None = 2,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    backlog = _load_json(Path(backlog_path or DEFAULT_BACKLOG_PATH))
    out_base = (
        Path(out_dir).resolve()
        if out_dir
        else REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
    )
    out_base.mkdir(parents=True, exist_ok=True)
    owns_session = session is None
    current_session = session or requests.Session()
    current_session.headers.update(DEFAULT_HEADERS)
    try:
        payload = build_reference_evidence(
            backlog,
            session=current_session,
            page_size=page_size,
            max_pages=max_pages,
        )
    finally:
        if owns_session:
            current_session.close()

    json_path = out_base / "msrp_source_reference_evidence.json"
    md_path = out_base / "msrp_source_reference_evidence.md"
    json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_markdown(payload, md_path)
    print(f"[reference] JSON: {json_path}")
    print(f"[reference] Markdown: {md_path}")
    print(
        "[reference] "
        f"{payload['summary']['localReferenceCount']} local references across "
        f"{payload['summary']['evidenceItemCount']} evidence items"
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build review-only MSRP reference evidence from source repair backlog."
    )
    parser.add_argument("--backlog", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=2)
    args = parser.parse_args()
    run(
        backlog_path=args.backlog,
        out_dir=args.out_dir,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
