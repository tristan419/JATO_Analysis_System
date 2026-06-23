from typing import Literal

from fastapi import APIRouter, Depends, Query

from app.api.schemas import (
    HeroProductDeckRequest,
    HeroProductPriceOverrideRequest,
    HeroProductSpecOverrideRequest,
    MarketScanDeckRequest,
    PositioningPricingDeckRequest,
    VersionComparisonDeckRequest,
)
from app.core.security import UserContext, optional_viewer, require_min_role
from app.db.session import get_db_session
from sqlalchemy.orm import Session
from app.services.customer_insight_service import (
    query_nordic_customer_deck,
    query_nordic_hev_customer_deck,
)
from app.services.hero_product_analysis_service import (
    query_hero_product_deck,
    upsert_hero_product_price_override,
    upsert_hero_product_spec_override,
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
    _=Depends(optional_viewer),
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
        drilldown_segments=payload.drilldown_segments,
        body_types=payload.body_types,
        view=payload.view,
    )


@router.post("/positioning-pricing-deck")
def positioning_pricing_deck(
    payload: PositioningPricingDeckRequest,
    _=Depends(optional_viewer),
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
    _=Depends(optional_viewer),
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


@router.post("/hero-product-deck")
def hero_product_deck(
    payload: HeroProductDeckRequest,
    _=Depends(optional_viewer),
) -> dict:
    return query_hero_product_deck(
        countries=payload.countries,
        price_country=payload.price_country,
        tracking_country=payload.tracking_country,
        target_period=payload.target_period,
        time_range=payload.time_range,
        sales_mode=payload.sales_mode,
        segment=payload.segment,
        fuel_type=payload.fuel_type,
        price_source=payload.price_source,
        top_n=payload.top_n,
        ranking_limit=payload.ranking_limit,
        country_limit=payload.country_limit,
        trend_window_months=payload.trend_window_months,
        top_models=payload.top_models,
        hero_models=payload.hero_models,
    )


@router.patch("/hero-product-price")
def patch_hero_product_price(
    payload: HeroProductPriceOverrideRequest,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    return {
        "item": upsert_hero_product_price_override(
            session,
            payload.model_dump(),
            updated_by=user.name,
        )
    }


@router.patch("/hero-product-spec")
def patch_hero_product_spec(
    payload: HeroProductSpecOverrideRequest,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    return {
        "item": upsert_hero_product_spec_override(
            session,
            payload.model_dump(),
            updated_by=user.name,
        )
    }


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
    sort_by: str = Query(default="sales"),
    _=Depends(optional_viewer),
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
        sort_by=sort_by,
    )


@router.get("/nordic-customer-deck")
def nordic_customer_deck(
    mode: Literal["benchmark", "forum_live"] = "benchmark",
    countries: list[str] | None = Query(default=None),
    _=Depends(optional_viewer),
) -> dict:
    return query_nordic_customer_deck(mode=mode, country_codes=countries)


@router.get("/nordic-hev-customer-deck")
def nordic_hev_customer_deck(
    mode: Literal["benchmark", "forum_live"] = "benchmark",
    countries: list[str] | None = Query(default=None),
    _=Depends(optional_viewer),
) -> dict:
    return query_nordic_hev_customer_deck(mode=mode, country_codes=countries)
