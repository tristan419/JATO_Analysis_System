from __future__ import annotations

from collections import Counter
from typing import Any

from app.scraper.source_bootstrap import (
    load_candidate_scope_report,
    rank_source_draft_opportunities,
)
from app.services.country_service import (
    country_filter_aliases,
    to_display_country,
)


COUNTRY_SCOPE_LIMIT: int | None = None
BACKLOG_LIMIT: int | None = None


def _normalize(value: str | None) -> str:
    return str(value or "").strip().casefold()


def _matches_country_filter(country: str, country_filter: str | None) -> bool:
    normalized_filter = _normalize(country_filter)
    if not normalized_filter:
        return True
    return _normalize(country) in country_filter_aliases(country_filter or "")


def _matches_brand_filter(brand: str, brand_filter: str | None) -> bool:
    normalized_filter = _normalize(brand_filter)
    if not normalized_filter:
        return True
    normalized_brand = _normalize(brand)
    return (
        normalized_brand == normalized_filter
        or normalized_filter in normalized_brand
    )


def _apply_limit(
    items: list[dict[str, Any]],
    limit: int | None,
) -> list[dict[str, Any]]:
    if limit is None:
        return items
    if limit <= 0:
        return []
    return items[:limit]


def _count_backlog_opportunities(report: dict[str, Any]) -> int:
    grouped_keys: set[tuple[str, str]] = set()
    for country_summary in report.get("country_summaries") or []:
        country = str(country_summary.get("country") or "").strip()
        if not country:
            continue
        for candidate in country_summary.get("candidates") or []:
            if str(candidate.get("coverage_status") or "") != "missing_source":
                continue
            brand = str(candidate.get("brand") or "").strip()
            if brand:
                grouped_keys.add((country, brand))
    return len(grouped_keys)


def build_country_scope_items(
    report: dict[str, Any],
    country_filter: str | None = None,
    brand_filter: str | None = None,
    limit: int | None = COUNTRY_SCOPE_LIMIT,
) -> list[dict[str, Any]]:
    if limit is not None and limit <= 0:
        return []

    items: list[dict[str, Any]] = []
    for country_summary in report.get("country_summaries") or []:
        raw_country = str(country_summary.get("country") or "").strip()
        if (
            not raw_country
            or not _matches_country_filter(raw_country, country_filter)
        ):
            continue

        candidates = [
            candidate
            for candidate in country_summary.get("candidates") or []
            if _matches_brand_filter(
                str(candidate.get("brand") or ""),
                brand_filter,
            )
        ]
        if not candidates:
            continue

        missing_candidates = [
            candidate
            for candidate in candidates
            if str(candidate.get("coverage_status") or "") == "missing_source"
        ]
        brand_counter = Counter(
            str(candidate.get("brand") or "").strip()
            for candidate in missing_candidates
            if str(candidate.get("brand") or "").strip()
        )
        top_missing_brands = [
            f"{brand} ({count})" if count > 1 else brand
            for brand, count in brand_counter.most_common(3)
        ]
        top_missing_models = [
            str(candidate.get("model") or "").strip()
            for candidate in missing_candidates[:5]
            if str(candidate.get("model") or "").strip()
        ]
        items.append(
            {
                "country": to_display_country(raw_country),
                "latestMonth": str(country_summary.get("latest_month") or ""),
                "windowStartMonth": str(
                    country_summary.get("window_start_month") or ""
                ),
                "windowEndMonth": str(
                    country_summary.get("window_end_month") or ""
                ),
                "candidateCount": len(candidates),
                "missingCount": len(missing_candidates),
                "topMissingBrands": top_missing_brands,
                "topMissingModels": top_missing_models,
            }
        )

    items.sort(
        key=lambda item: (
            -int(item["missingCount"]),
            -int(item["candidateCount"]),
            str(item["country"]),
        )
    )
    return _apply_limit(items, limit)


def build_backlog_items(
    report: dict[str, Any],
    country_filter: str | None = None,
    brand_filter: str | None = None,
    limit: int | None = BACKLOG_LIMIT,
) -> list[dict[str, Any]]:
    if limit is not None and limit <= 0:
        return []

    opportunities = rank_source_draft_opportunities(
        report,
        batch_size=_count_backlog_opportunities(report),
    )
    filtered = [
        opportunity
        for opportunity in opportunities
        if _matches_country_filter(opportunity.country, country_filter)
        and _matches_brand_filter(opportunity.brand, brand_filter)
    ]

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for opportunity in filtered:
        group_key = (opportunity.country, opportunity.brand)
        current = grouped.get(group_key)
        if current is None:
            current = {
                "country": opportunity.country,
                "countryCode": opportunity.country_code,
                "brand": opportunity.brand,
                "brandSlug": opportunity.brand_slug,
                "countryModelRank": opportunity.country_model_rank,
                "candidateModelCount": 0,
                "sales12mSum": 0.0,
                "topModels": [],
            }
            grouped[group_key] = current

        current["countryModelRank"] = min(
            int(current["countryModelRank"]),
            int(opportunity.country_model_rank),
        )
        current["candidateModelCount"] = (
            int(current["candidateModelCount"]) + 1
        )
        current["sales12mSum"] = (
            float(current["sales12mSum"]) + float(opportunity.sales_12m)
        )
        if opportunity.model not in current["topModels"]:
            current["topModels"].append(opportunity.model)

    ranked_groups = sorted(
        grouped.values(),
        key=lambda item: (
            int(item["countryModelRank"]),
            str(item["country"]),
            str(item["brand"]),
        ),
    )

    items = []
    for index, item in enumerate(ranked_groups, start=1):
        brand_slug = str(item["brandSlug"])
        country_code = str(item["countryCode"])
        file_name = f"{index:02d}_{brand_slug}_{country_code}.yaml"
        items.append(
            {
                "priorityRank": index,
                "country": to_display_country(str(item["country"])),
                "countryCode": country_code,
                "brand": str(item["brand"]),
                "brandSlug": brand_slug,
                "candidateModelCount": int(item["candidateModelCount"]),
                "sales12mSum": float(item["sales12mSum"]),
                "topModels": list(item["topModels"]),
                "sourceCode": f"{brand_slug}_{country_code}_draft_scrapling",
                "fileName": file_name,
                "relativePath": f"{country_code}/{file_name}",
            }
        )
    return _apply_limit(items, limit)


def get_review_workbench(
    country: str | None = None,
    brand: str | None = None,
) -> dict[str, Any]:
    try:
        report = load_candidate_scope_report()
    except (FileNotFoundError, ValueError):
        return {
            "candidateScopeAvailable": False,
            "backlogAvailable": False,
            "generatedAtUtc": None,
            "reportTopN": 0,
            "countryCount": 0,
            "candidateCount": 0,
            "coverageSummary": {
                "modelSource": 0,
                "brandSource": 0,
                "missingSource": 0,
            },
            "countryScope": [],
            "backlog": [],
        }

    raw_coverage_summary = report.get("coverage_summary")
    coverage_summary = (
        raw_coverage_summary if isinstance(raw_coverage_summary, dict) else {}
    )

    backlog_available = True
    try:
        backlog = build_backlog_items(report, country, brand)
    except (KeyError, AttributeError):
        backlog = []
        backlog_available = False

    return {
        "candidateScopeAvailable": True,
        "backlogAvailable": backlog_available,
        "generatedAtUtc": report.get("generated_at_utc"),
        "reportTopN": int(report.get("top_n") or 0),
        "countryCount": int(report.get("country_count") or 0),
        "candidateCount": int(report.get("candidate_count") or 0),
        "coverageSummary": {
            "modelSource": int(coverage_summary.get("model_source") or 0),
            "brandSource": int(coverage_summary.get("brand_source") or 0),
            "missingSource": int(coverage_summary.get("missing_source") or 0),
        },
        "countryScope": build_country_scope_items(report, country, brand),
        "backlog": backlog,
    }
