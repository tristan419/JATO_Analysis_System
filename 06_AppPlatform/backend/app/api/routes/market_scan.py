from typing import Literal

from fastapi import APIRouter, Depends, Query

from app.api.schemas import (
    MarketScanDeckRequest,
    PositioningPricingDeckRequest,
    VersionComparisonDeckRequest,
)
from app.core.security import require_min_role
from app.services.customer_insight_service import (
    query_nordic_customer_deck,
    query_nordic_hev_customer_deck,
)
from app.services.market_scan_service import (
    query_market_scan_deck,
    query_ranking_trend,
    query_positioning_pricing_deck,
    query_version_comparison_deck,
)

router = APIRouter(prefix="/market-scan", tags=["market-scan"])


@router.post("/deck")
def market_scan_deck(
    payload: MarketScanDeckRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_market_scan_deck(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        trend_window_months=payload.trend_window_months,
        origin_window_months=payload.origin_window_months,
        body_window_months=payload.body_window_months,
        ranking_limit=payload.ranking_limit,
        drilldown_segment=payload.drilldown_segment,
    )


@router.post("/positioning-pricing-deck")
def positioning_pricing_deck(
    payload: PositioningPricingDeckRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_positioning_pricing_deck(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        sales_mode=payload.sales_mode,
        top_n=payload.top_n,
        msrp_min=payload.msrp_min,
        msrp_max=payload.msrp_max,
        length_min=payload.length_min,
        length_max=payload.length_max,
        price_band_size=payload.price_band_size,
    )


@router.post("/version-comparison-deck")
def version_comparison_deck(
    payload: VersionComparisonDeckRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_version_comparison_deck(
        country=payload.country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        fuel_types=payload.fuel_types,
        sales_mode=payload.sales_mode,
        comparison_mode=payload.comparison_mode,
        segment=payload.segment,
        models=payload.models,
        msrp_min=payload.msrp_min,
        msrp_max=payload.msrp_max,
        price_band_size=payload.price_band_size,
        body_type=payload.body_type,
        drive_types=payload.drive_types,
        segments=payload.segments,
        length_min=payload.length_min,
        length_max=payload.length_max,
    )


@router.get("/ranking-trend")
def ranking_trend(
    country: str = Query(...),
    brand: str = Query(...),
    model: str | None = Query(default=None),
    segment: str | None = Query(default=None),
    source_table: str = Query(default="monthly_brand_ranking"),
    fuel_types: list[str] | None = Query(default=None),
    msrp_min: float | None = Query(default=None),
    msrp_max: float | None = Query(default=None),
    length_min: float | None = Query(default=None),
    length_max: float | None = Query(default=None),
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_ranking_trend(
        country=country,
        brand=brand,
        model=model,
        segment=segment,
        source_table=source_table,
        fuel_types=fuel_types,
        msrp_min=msrp_min,
        msrp_max=msrp_max,
        length_min=length_min,
        length_max=length_max,
    )


@router.get("/nordic-customer-deck")
def nordic_customer_deck(
    mode: Literal["benchmark", "forum_live"] = "benchmark",
    countries: list[str] | None = Query(default=None),
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_nordic_customer_deck(mode=mode, country_codes=countries)


@router.get("/nordic-hev-customer-deck")
def nordic_hev_customer_deck(
    mode: Literal["benchmark", "forum_live"] = "benchmark",
    countries: list[str] | None = Query(default=None),
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_nordic_hev_customer_deck(mode=mode, country_codes=countries)
