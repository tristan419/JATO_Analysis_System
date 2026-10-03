"""Fine-grained query tools exposing JATO filter + lens capabilities as MCP tools.

These wrap the existing query_service, country_chat_service, and market_scan_service
with filter parameters matching the frontend CollapsibleFilterSidebar + Analysis Deck lenses.
"""

from __future__ import annotations

import re
from typing import Any

from app.services.country_chat_service import (
    build_country_chart_deck,
    build_country_snapshot,
    extract_user_params,
    infer_country_chat_intents,
    _build_country_chat_route,
    _enrich_snapshot_for_intents,
)
from app.services import query_service
from app.services import msrp_lookup_service
from app.services import engineering_variant_diff_service


def query_with_filters(
    country: str,
    powertrain: str = "",
    fuel_type: str = "",
    segment: str = "",
    brand: str = "",
    model: str = "",
    year: int | None = None,
    metric: str = "sales",
    top_n: int = 10,
) -> dict[str, Any]:
    """Unified query with all filter dimensions matching the frontend sidebar.

    Args:
        country: Country name (e.g. "Sweden")
        powertrain: BEV, PHEV, HEV, ICE, MHEV
        fuel_type: Electric, Petrol, Diesel, Hybrid
        segment: SUV-B, SUV-C, Car-C, etc.
        brand: Brand name filter
        model: Model name filter
        year: Specific year (defaults to latest)
        metric: sales, share, ranking, trend
        top_n: Number of top results
    """
    user_params: dict[str, Any] = {}
    if year:
        user_params["year"] = year
    if brand:
        user_params["brand"] = brand
    if model:
        user_params["model"] = model

    snapshot = build_country_snapshot(country, user_params=user_params)

    # Apply additional filters
    filtered: dict[str, Any] = {"country": country, "appliedFilters": {}, "results": {}}

    # Powertrain mix filtered
    pm = snapshot.get("powertrainMix", [])
    if isinstance(pm, list) and powertrain:
        pm_filtered = [p for p in pm if powertrain.lower() in str(p.get("label", "")).lower()]
        filtered["results"]["powertrainMix"] = pm_filtered[:top_n]
        filtered["appliedFilters"]["powertrain"] = powertrain
    elif isinstance(pm, list):
        filtered["results"]["powertrainMix"] = pm[:top_n]

    # Top models (with optional brand/model/fuel filter)
    top_models = snapshot.get("topModels", [])
    if isinstance(top_models, list):
        filtered_list = top_models
        if brand:
            filtered_list = [m for m in filtered_list if brand.lower() in str(m.get("label", "")).lower()]
        if model:
            filtered_list = [m for m in filtered_list if model.lower() in str(m.get("label", "")).lower()]
        filtered["results"]["topModels"] = filtered_list[:top_n]
        if brand:
            filtered["appliedFilters"]["brand"] = brand
        if model:
            filtered["appliedFilters"]["model"] = model

    # Top brands
    top_brands = snapshot.get("topBrands", [])
    if isinstance(top_brands, list):
        filtered["results"]["topBrands"] = top_brands[:top_n]

    # Year series (trend data)
    year_series = snapshot.get("yearSeries", [])
    if isinstance(year_series, list):
        filtered["results"]["yearSeries"] = year_series

    # Month series
    month_series = snapshot.get("monthSeries", [])
    if isinstance(month_series, list):
        filtered["results"]["monthSeries"] = month_series[-12:]  # Last 12 months

    # KPIs
    filtered["results"]["kpis"] = snapshot.get("kpis", {})
    filtered["results"]["analysisMeta"] = snapshot.get("analysisMeta", {})

    if year:
        filtered["appliedFilters"]["year"] = year

    return filtered


def query_time_series(
    country: str,
    metric: str = "sales",
    powertrain: str = "",
    fuel_type: str = "",
    segment: str = "",
    year: int | None = None,
    granularity: str = "monthly",
) -> dict[str, Any]:
    """Time-series lens: get trend data with optional dimension filters.

    Args:
        country: Country
        metric: sales, share, bev_share, phev_share, growth
        powertrain: Filter by powertrain type
        fuel_type: Filter by fuel type
        segment: Filter by segment
        year: Specific year
        granularity: monthly or yearly
    """
    normalized_granularity = "yearly" if str(granularity).lower() in {"year", "yearly", "annual"} else "monthly"
    filters, unresolved_filters = _time_series_filters(
        country=country,
        powertrain=powertrain,
        fuel_type=fuel_type,
        segment=segment,
    )
    raw = query_service.query_time_series(
        filters=filters,
        grain="year" if normalized_granularity == "yearly" else "month",
        top_n=12 if normalized_granularity == "yearly" else 36,
    )
    raw_items = raw.get("items") if isinstance(raw, dict) and isinstance(raw.get("items"), list) else []
    if year:
        year_prefix = str(year)
        raw_items = [
            item
            for item in raw_items
            if isinstance(item, dict) and str(item.get("time") or "").startswith(year_prefix)
        ]

    # The wide JATO dataset contains registration volumes. Do not relabel those
    # values as share/growth when a caller asks for an unavailable metric.
    points = [
        {
            "period": str(item.get("time") or ""),
            "sales": float(item.get("value") or 0),
        }
        for item in raw_items
        if isinstance(item, dict) and str(item.get("time") or "").strip()
    ]
    series_key = "monthSeries" if normalized_granularity == "monthly" else "yearSeries"
    requested_metric = str(metric or "sales").strip().lower() or "sales"
    result: dict[str, Any] = {
        "country": country,
        "metric": "sales",
        "requestedMetric": requested_metric,
        "granularity": normalized_granularity,
        "appliedFilters": {
            "country": country,
            "powertrain": powertrain,
            "fuelType": fuel_type,
            "segment": segment,
            "year": year,
        },
        "resolvedDatasetFilters": filters,
        "series": points,
        series_key: points,
        "dataPoints": len(points),
        "stats": _time_series_stats(points),
    }
    diagnostics: dict[str, Any] = {}
    if unresolved_filters:
        diagnostics["unresolvedFilters"] = unresolved_filters
    if requested_metric not in {"sales", "volume", "registrations", "registration"}:
        diagnostics["metricBoundary"] = (
            f"requested {requested_metric}; returned registration sales because the current Parquet series does not contain a verified {requested_metric} series"
        )
    if not points:
        diagnostics["diagnosis"] = "no_time_series_rows_for_requested_scope"
    if diagnostics:
        result["coverageDiagnostics"] = diagnostics
    return result


def _time_series_filters(
    *,
    country: str,
    powertrain: str,
    fuel_type: str,
    segment: str,
) -> tuple[dict[str, list[str]], list[str]]:
    columns = query_service.repo.list_columns()
    filters: dict[str, list[str]] = {}
    unresolved: list[str] = []
    dimensions = (
        ("country", query_service.COUNTRY_CANDIDATES, [country]),
        ("powertrain", query_service.POWERTRAIN_CANDIDATES, _split_dimension_values(powertrain or fuel_type)),
        ("segment", query_service.SEGMENT_CANDIDATES, _split_dimension_values(segment)),
    )
    for name, candidates, values in dimensions:
        cleaned_values = [str(value or "").strip() for value in values if str(value or "").strip()]
        if not cleaned_values:
            continue
        column = query_service._resolve_existing_column(candidates, columns)
        if not column:
            unresolved.append(name)
            continue
        filters[column] = cleaned_values
    return filters, unresolved


def _split_dimension_values(value: str) -> list[str]:
    return [
        item.strip()
        for item in re.split(r"[,/|，、]+", str(value or ""))
        if item.strip()
    ]


def _time_series_stats(points: list[dict[str, Any]]) -> dict[str, Any]:
    if not points:
        return {}
    first = points[0]
    last = points[-1]
    start_value = float(first.get("sales") or 0)
    end_value = float(last.get("sales") or 0)
    change = end_value - start_value
    return {
        "startPeriod": first.get("period"),
        "endPeriod": last.get("period"),
        "startSales": start_value,
        "endSales": end_value,
        "changeSales": change,
        "changePct": round((change / start_value) * 100, 2) if start_value else None,
    }


def query_segment_breakdown(
    country: str,
    segment: str = "",
    powertrain: str = "",
    year: int | None = None,
) -> dict[str, Any]:
    """Segment lens: break down market by segment with cross-tab capabilities.

    Args:
        country: Country
        segment: Specific segment to focus on (empty = all)
        powertrain: Cross-filter by powertrain
        year: Year
    """
    user_params: dict[str, Any] = {}
    if year:
        user_params["year"] = year

    # Get snapshot with cross-tab data
    snapshot = build_country_snapshot(country, user_params=user_params)
    cross_tabs = snapshot.get("crossTabs", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(cross_tabs, dict):
        cross_tabs = {}

    result: dict[str, Any] = {
        "country": country,
        "appliedFilters": {"segment": segment, "powertrain": powertrain, "year": year},
    }

    # Cross-tab dimensions
    for dim_key in ("driveByFuel", "driveBySegment", "segmentByFuel", "fuelBySegment", "registrationByFuel", "registrationBySegment"):
        data = cross_tabs.get(dim_key, [])
        if isinstance(data, list) and data:
            data = _filter_segment_breakdown_rows(data, dim_key=dim_key, segment=segment, powertrain=powertrain)
            result[dim_key] = data[:12]

    # Top models per segment
    top_models = snapshot.get("topModels", []) if isinstance(snapshot, dict) else []
    if isinstance(top_models, list) and segment:
        result["topModelsInSegment"] = [m for m in top_models if segment.lower() in str(m.get("label", "")).lower()][:10]

    result["kpis"] = snapshot.get("kpis", {}) if isinstance(snapshot, dict) else {}

    return result


def _filter_segment_breakdown_rows(
    rows: list[Any],
    *,
    dim_key: str,
    segment: str = "",
    powertrain: str = "",
) -> list[Any]:
    filtered = [row for row in rows if isinstance(row, dict)]
    segment_token = str(segment or "").strip().lower()
    powertrain_token = str(powertrain or "").strip().lower()
    if segment_token and dim_key in {"driveBySegment", "segmentByFuel", "registrationBySegment"}:
        filtered = [row for row in filtered if segment_token in _cross_tab_row_label(row).lower()]
    if powertrain_token:
        if dim_key in {"driveByFuel", "registrationByFuel", "fuelBySegment"}:
            filtered = [row for row in filtered if _cross_tab_row_label(row).lower() == powertrain_token]
        elif dim_key == "segmentByFuel":
            metric_key = f"{powertrain_token}_pct"
            filtered = [
                row for row in filtered
                if any(str(key).lower() == metric_key for key in row.keys())
            ]
    return filtered


def _cross_tab_row_label(row: dict[str, Any]) -> str:
    return str(row.get("label") or row.get("_index") or row.get("name") or "").strip()


def query_price_positioning(
    country: str,
    model: str = "",
    brand: str = "",
    powertrain: str = "",
    top_n: int = 10,
) -> dict[str, Any]:
    """Price positioning lens: price distribution and competitive positioning.

    Args:
        country: Country
        model: Specific model to analyze
        brand: Brand filter
        powertrain: Powertrain filter
        top_n: Max results
    """
    result: dict[str, Any] = {
        "country": country,
        "appliedFilters": {"model": model, "brand": brand, "powertrain": powertrain},
    }

    # MSRP data
    try:
        msrp_data = msrp_lookup_service.lookup_current_msrp_from_db(
            country=country,
            brand=brand or None,
            model=model or None,
            powertrain=powertrain or None,
            max_items=top_n,
        )
        items = msrp_data.get("items", []) if isinstance(msrp_data, dict) else []
        result["priceRecords"] = items[:top_n]
        result["recordCount"] = len(items)

        # Price stats
        prices = []
        for item in items:
            for k in ("msrp", "retailPrice", "price", "basePrice"):
                v = item.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    prices.append(v)
                    break
        if prices:
            result["priceStats"] = {
                "min": min(prices),
                "max": max(prices),
                "avg": sum(prices) / len(prices),
                "median": sorted(prices)[len(prices) // 2],
                "count": len(prices),
            }
    except Exception:
        result["priceRecords"] = []
        result["recordCount"] = 0

    # Also get snapshot rankings for context
    try:
        snapshot = build_country_snapshot(country)
        if isinstance(snapshot, dict):
            result["marketContext"] = {
                "kpis": snapshot.get("kpis", {}),
                "topModels": snapshot.get("topModels", [])[:8],
            }
    except Exception:
        pass

    return result


def query_competitive_landscape(
    country: str,
    model: str,
    include_pricing: bool = True,
    include_features: bool = True,
    competitor_count: int = 5,
) -> dict[str, Any]:
    """One-stop competitive intelligence: given a model, find and compare competitors.

    Args:
        country: Country
        model: Target model name
        include_pricing: Include MSRP comparison
        include_features: Include feature/config comparison
        competitor_count: Number of competitors to compare
    """
    result: dict[str, Any] = {"country": country, "model": model, "competitors": []}

    # Get market context
    try:
        snapshot = build_country_snapshot(country)
        if not isinstance(snapshot, dict):
            return result
    except Exception:
        return result

    top_models = snapshot.get("topModels", [])
    if not isinstance(top_models, list):
        return result

    # Find target model and nearby competitors
    target_idx = None
    for i, m in enumerate(top_models):
        if model.lower() in str(m.get("label", "")).lower():
            target_idx = i
            result["targetRank"] = i + 1
            result["targetSales"] = m.get("value", 0)
            break

    if target_idx is None:
        return result

    start = max(0, target_idx - 2)
    end = min(len(top_models), target_idx + competitor_count + 1)
    competitor_models = top_models[start:end]

    for comp in competitor_models:
        comp_name = str(comp.get("label", ""))
        comp_data: dict[str, Any] = {"model": comp_name, "sales": comp.get("value", 0)}

        if include_pricing:
            try:
                pricing = msrp_lookup_service.lookup_current_msrp_from_db(
                    country=country, model=comp_name, max_items=3,
                )
                items = pricing.get("items", []) if isinstance(pricing, dict) else []
                if items:
                    prices = []
                    for item in items:
                        for k in ("msrp", "retailPrice", "price"):
                            v = item.get(k)
                            if isinstance(v, (int, float)) and v > 0:
                                prices.append(v)
                                break
                    if prices:
                        comp_data["avgPrice"] = sum(prices) / len(prices)
                        comp_data["priceRange"] = f"{min(prices):.0f}-{max(prices):.0f}"
            except Exception:
                pass

        if include_features and comp_name != model:
            try:
                diff = engineering_variant_diff_service.compare_market_variants_from_db(
                    country=country, model=comp_name, max_subjects=1, max_diff_features=5, max_common_features=3,
                )
                if isinstance(diff, dict):
                    comp_data["keyFeatures"] = (diff.get("diffFeatures") or [])[:3]
            except Exception:
                pass

        result["competitors"].append(comp_data)

    result["competitorCount"] = len(result["competitors"])
    return result
