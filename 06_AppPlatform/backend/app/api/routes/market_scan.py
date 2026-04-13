from fastapi import APIRouter, Depends

from app.api.schemas import MarketScanDeckRequest
from app.core.security import require_min_role
from app.services.market_scan_service import query_market_scan_deck

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
