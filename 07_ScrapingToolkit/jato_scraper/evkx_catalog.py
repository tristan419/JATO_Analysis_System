"""Fetch BEV MSRP and specification snapshots from EVKX."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from html import unescape
import json
import re
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests

EVKX_BASE_URL = "https://evkx.net/"
EVKX_SEARCH_API_URL = urljoin(EVKX_BASE_URL, "api/evs/search")
DEFAULT_HEADERS = {
    "User-Agent": "JATO-EVKX-Fetcher/0.1",
    "Accept": "application/json, text/html;q=0.9",
}
SECTION_HEADING_RE = re.compile(
    r"<!--\s*[A-Za-z0-9 /&-]+Section\s*-->\s*<section\b.*?<h2[^>]*>(.*?)</h2>",
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
PRICING_SECTION_RE = re.compile(
    r"<h3[^>]*>\s*Pricing\s*</h3>.*?<ul[^>]*>(.*?)</ul>",
    re.IGNORECASE | re.DOTALL,
)
PRICING_ITEM_RE = re.compile(r"<li[^>]*>(.*?)</li>", re.IGNORECASE | re.DOTALL)
PAREN_SUFFIX_RE = re.compile(r"^(.*?)\s*\(([^()]+)\)\s*$", re.DOTALL)
JSON_LD_RE = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>\s*(.*?)\s*</script>',
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class EvkxFetchOptions:
    pricing_country: str = "UnitedStates"
    availability_filter: str = "current"
    page_size: int = 100
    max_pages: int | None = None
    limit: int | None = None
    include_details: bool = True


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_output_path(pricing_country: str, availability_filter: str) -> Path:
    safe_country = re.sub(r"[^a-z0-9]+", "_", pricing_country.lower()).strip("_")
    safe_availability = re.sub(
        r"[^a-z0-9]+", "_", availability_filter.lower()
    ).strip("_")
    return (
        _repo_root()
        / "04_Processed_data"
        / "msrp_candidate_scope"
        / "evkx"
        / f"evkx_bev_{safe_country}_{safe_availability}.json"
    )


def _clean_text(value: str) -> str:
    text = TAG_RE.sub(" ", unescape(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def _normalize_amount(raw_value: str) -> float | None:
    value = raw_value.replace("\xa0", " ").strip()
    number_match = re.search(r"[\d][\d,.\s]*", value)
    if not number_match:
        return None
    number_text = number_match.group(0).replace(" ", "")
    if "," in number_text and "." in number_text:
        if number_text.rfind(",") > number_text.rfind("."):
            normalized = number_text.replace(".", "").replace(",", ".")
        else:
            normalized = number_text.replace(",", "")
    elif "," in number_text:
        parts = number_text.split(",")
        normalized = (
            number_text.replace(",", ".")
            if len(parts[-1]) <= 2
            else number_text.replace(",", "")
        )
    else:
        normalized = number_text
    try:
        return float(normalized)
    except ValueError:
        return None


def _infer_currency_code(price_text: str, market_label: str | None = None) -> str | None:
    normalized = price_text.strip()
    upper_market = (market_label or "").strip().upper()
    if normalized.startswith("CA$"):
        return "CAD"
    if normalized.startswith("A$"):
        return "AUD"
    if normalized.startswith("NZ$"):
        return "NZD"
    if normalized.startswith("R$"):
        return "BRL"
    if normalized.startswith("MX$"):
        return "MXN"
    if normalized.startswith("$"):
        if upper_market in {"CANADA"}:
            return "CAD"
        return "USD"
    if "€" in normalized:
        return "EUR"
    if "£" in normalized:
        return "GBP"
    if "NOK" in normalized:
        return "NOK"
    if "DKK" in normalized:
        return "DKK"
    if "SEK" in normalized:
        return "SEK"
    if "PLN" in normalized:
        return "PLN"
    if "CNY" in normalized:
        return "CNY"
    return None


def absolute_evkx_url(path_or_url: str) -> str:
    return urljoin(EVKX_BASE_URL, path_or_url)


def parse_pricing_section(detail_html: str) -> list[dict[str, Any]]:
    match = PRICING_SECTION_RE.search(detail_html)
    if not match:
        return []

    items: list[dict[str, Any]] = []
    for raw_item in PRICING_ITEM_RE.findall(match.group(1)):
        item_text = _clean_text(raw_item)
        if not item_text:
            continue
        market_label = None
        price_text = item_text
        paren_match = PAREN_SUFFIX_RE.match(item_text)
        if paren_match:
            price_text = paren_match.group(1).strip()
            market_label = paren_match.group(2).strip()
        items.append(
            {
                "marketLabel": market_label,
                "priceText": price_text,
                "amount": _normalize_amount(price_text),
                "currency": _infer_currency_code(price_text, market_label),
            }
        )
    return items


def parse_json_ld(detail_html: str) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for raw_blob in JSON_LD_RE.findall(detail_html):
        blob = raw_blob.strip()
        if not blob:
            continue
        try:
            payload = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            parsed.extend(item for item in payload if isinstance(item, dict))
        elif isinstance(payload, dict):
            parsed.append(payload)
    return parsed


def parse_specifications_page(spec_html: str) -> dict[str, dict[str, str]]:
    section_titles = [
        _clean_text(match.group(1))
        for match in SECTION_HEADING_RE.finditer(spec_html)
    ]
    tables = pd.read_html(StringIO(spec_html))
    sections: dict[str, dict[str, str]] = {}
    for title, table in zip(section_titles, tables):
        if table.shape[1] < 2:
            continue
        normalized_table = table.fillna("")
        pairs: dict[str, str] = {}
        for _, row in normalized_table.iterrows():
            key = _clean_text(str(row.iloc[0]))
            value = _clean_text(str(row.iloc[1]))
            if not key or key.lower() == "spec":
                continue
            pairs[key] = value
        if pairs:
            sections[title] = pairs
    return sections


def fetch_search_page(
    session: requests.Session,
    *,
    pricing_country: str,
    availability_filter: str,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    response = session.post(
        EVKX_SEARCH_API_URL,
        json={
            "page": page,
            "pageSize": page_size,
            "sortOrder": "Name",
            "availabilityFilter": availability_filter,
            "pricingCountry": pricing_country,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected EVKX search payload type")
    return payload


def fetch_search_catalog(
    session: requests.Session,
    options: EvkxFetchOptions,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = fetch_search_page(
            session,
            pricing_country=options.pricing_country,
            availability_filter=options.availability_filter,
            page=page,
            page_size=options.page_size,
        )
        page_items = payload.get("evs") or []
        if not isinstance(page_items, list):
            raise ValueError("EVKX search payload missing 'evs' list")
        items.extend(item for item in page_items if isinstance(item, dict))

        if options.limit is not None and len(items) >= options.limit:
            return items[: options.limit]

        if options.max_pages is not None and page >= options.max_pages:
            return items

        if not payload.get("hasNextPage"):
            return items
        page += 1


def fetch_vehicle_detail(
    session: requests.Session,
    info_url: str,
) -> dict[str, Any]:
    detail_response = session.get(info_url, timeout=60)
    detail_response.raise_for_status()
    detail_html = detail_response.text

    spec_url = urljoin(info_url.rstrip("/") + "/", "specifications/")
    spec_response = session.get(spec_url, timeout=60)
    spec_response.raise_for_status()
    spec_html = spec_response.text

    return {
        "pricingByMarket": parse_pricing_section(detail_html),
        "specifications": parse_specifications_page(spec_html),
        "schemaOrg": parse_json_ld(detail_html),
        "specificationsUrl": spec_url,
    }


def build_catalog_record(
    search_item: dict[str, Any],
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    info_url = absolute_evkx_url(str(search_item.get("infoUri") or ""))
    record = {
        "evId": search_item.get("evId"),
        "name": search_item.get("name"),
        "infoUrl": info_url,
        "thumbnailUrl": search_item.get("thumbUri"),
        "pricingCountry": search_item.get("pricingCountry"),
        "startPrice": search_item.get("startPrice"),
        "currency": search_item.get("currency"),
        "currencySymbol": search_item.get("currencySymbol"),
        "isConverted": search_item.get("isConverted"),
        "searchSummary": {
            key: value
            for key, value in search_item.items()
            if key not in {"evId", "name", "infoUri", "thumbUri"}
        },
    }
    if detail:
        record.update(detail)
    return record


def fetch_evkx_catalog(
    options: EvkxFetchOptions,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    owns_session = session is None
    current_session = session or requests.Session()
    current_session.headers.update(DEFAULT_HEADERS)
    try:
        search_items = fetch_search_catalog(current_session, options)
        records = []
        for item in search_items:
            detail = None
            if options.include_details:
                detail = fetch_vehicle_detail(
                    current_session,
                    absolute_evkx_url(str(item.get("infoUri") or "")),
                )
            records.append(build_catalog_record(item, detail))
        return {
            "metadata": {
                "source": "EVKX",
                "baseUrl": EVKX_BASE_URL,
                "searchApiUrl": EVKX_SEARCH_API_URL,
                "pricingCountry": options.pricing_country,
                "availabilityFilter": options.availability_filter,
                "pageSize": options.page_size,
                "recordCount": len(records),
                "detailsIncluded": options.include_details,
            },
            "records": records,
        }
    finally:
        if owns_session:
            current_session.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch BEV MSRP and specification snapshots from EVKX"
    )
    parser.add_argument(
        "--pricing-country",
        default="UnitedStates",
        help="EVKX pricingCountry, e.g. UnitedStates, Germany, Finland",
    )
    parser.add_argument(
        "--availability-filter",
        default="current",
        help="EVKX availability filter, e.g. current, all, discontinued",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="API page size for EVKX search pagination",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on fetched search pages",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of vehicle records",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Only fetch the search API list; skip per-vehicle detail/spec pages",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path; defaults to 04_Processed_data/msrp_candidate_scope/evkx/",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    options = EvkxFetchOptions(
        pricing_country=args.pricing_country,
        availability_filter=args.availability_filter,
        page_size=args.page_size,
        max_pages=args.max_pages,
        limit=args.limit,
        include_details=not args.skip_details,
    )
    payload = fetch_evkx_catalog(options)
    output_path = args.output or _default_output_path(
        options.pricing_country,
        options.availability_filter,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
