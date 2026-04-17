from fastapi import APIRouter, Depends

from app.api.schemas import (
    MarketScanDeckRequest,
    PositioningPricingDeckRequest,
    VersionComparisonDeckRequest,
)
from app.core.security import require_min_role
from app.services.customer_insight_service import query_nordic_customer_deck
from app.services.market_scan_service import (
    query_market_scan_deck,
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
        fuel_types=payload.fuel_types,
        sales_mode=payload.sales_mode,
        top_n=payload.top_n,
        msrp_min=payload.msrp_min,
        msrp_max=payload.msrp_max,
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
        fuel_types=payload.fuel_types,
        sales_mode=payload.sales_mode,
        segment=payload.segment,
        models=payload.models,
        msrp_min=payload.msrp_min,
        msrp_max=payload.msrp_max,
        price_band_size=payload.price_band_size,
    )


@router.get("/nordic-customer-deck")
def nordic_customer_deck(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return query_nordic_customer_deck()
