from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.schemas import (
    CurrentPriceRemapRequest,
    CurrentPriceMaterializeRequest,
    ScrapeBatchIngestRequest,
)
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.msrp_workflow_service import (
    build_current_price_snapshot,
    build_multi_source_reconciliation,
    build_price_sales_effectiveness,
    create_scrape_batch_ingest,
    list_current_price_alerts,
    list_current_prices,
    list_finance_observations,
    list_price_history,
    materialize_current_prices,
    queue_reconciliation_conflicts_for_review,
    remap_current_price,
)
from app.services.msrp_monitoring_service import (
    build_msrp_backfill_snapshot_preview,
    build_msrp_monitoring_events,
)

router = APIRouter(prefix="/msrp", tags=["msrp"])


@router.post("/batches")
def post_scrape_batch(
    payload: ScrapeBatchIngestRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_scrape_batch_ingest(session, payload.model_dump())}


@router.get("/current-prices")
def get_current_prices(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_current_prices(
        session,
        country,
        brand,
        jato_model,
        limit,
        offset,
    )


@router.get("/monitoring/events")
def get_msrp_monitoring_events(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    window_days: int = Query(default=30, ge=1, le=365),
    from_date: date | None = Query(default=None),
    threshold_pct: float = Query(default=0.0, ge=0.0, le=50.0),
    direction: str = Query(default="drops", pattern="^(drops|increases|all)$"),
    limit: int = Query(default=500, ge=1, le=500),
    mode: str = Query(default="live", pattern="^(live|sweden_demo)$"),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return build_msrp_monitoring_events(
        session,
        country=country,
        brand=brand,
        jato_model=jato_model,
        window_days=window_days,
        from_date=from_date,
        threshold_pct=threshold_pct,
        direction=direction,
        limit=limit,
        mode=mode,
    )


@router.get("/monitoring/backfill-snapshot")
def get_msrp_backfill_snapshot(
    path: str = Query(min_length=1),
    max_chars: int = Query(default=20_000, ge=1_000, le=100_000),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return build_msrp_backfill_snapshot_preview(path, max_chars=max_chars)


@router.get("/finance-observations")
def get_finance_observations(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    price_semantics: str | None = Query(default=None),
    finance_type: str | None = Query(default=None),
    has_monthly_payment: bool | None = Query(default=None),
    has_subsidy: bool | None = Query(default=None),
    has_net_price_after_subsidy: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_finance_observations(
        session,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
        limit,
        offset,
    )


@router.get("/reconciliation")
def get_multi_source_reconciliation(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    threshold_pct: float = Query(default=1.0, ge=0.0, le=50.0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return build_multi_source_reconciliation(
        session,
        country,
        brand,
        jato_model,
        limit,
        threshold_pct,
    )


@router.post("/reconciliation/review-cases")
def post_reconciliation_review_cases(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    threshold_pct: float = Query(default=1.0, ge=0.0, le=50.0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": queue_reconciliation_conflicts_for_review(
            session,
            country,
            brand,
            jato_model,
            limit,
            threshold_pct,
        )
    }


@router.get("/effectiveness")
def get_price_sales_effectiveness(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    threshold_pct: float = Query(default=3.0, ge=0.0, le=50.0),
    baseline_window_months: int = Query(default=3, ge=1, le=12),
    post_window_months: int = Query(default=3, ge=1, le=12),
    post_lag_months: int = Query(default=1, ge=0, le=12),
    min_months: int = Query(default=1, ge=1, le=12),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return build_price_sales_effectiveness(
        session,
        country,
        brand,
        jato_model,
        limit,
        baseline_window_months,
        post_window_months,
        post_lag_months,
        min_months,
        threshold_pct=threshold_pct,
    )


@router.get("/current-prices/snapshot")
def get_current_price_snapshot(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=500),
    threshold_pct: float = Query(default=3.0, ge=0.0, le=50.0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return build_current_price_snapshot(
        session,
        country,
        brand,
        jato_model,
        limit,
        threshold_pct,
    )


@router.get("/current-prices/alerts")
def get_current_price_alerts(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    threshold_pct: float = Query(default=3.0, ge=0.0, le=50.0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        limit,
        offset,
        threshold_pct,
    )


@router.post("/current-prices/materialize")
def post_materialize_current_prices(
    payload: CurrentPriceMaterializeRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": materialize_current_prices(
            session,
            payload.country,
            payload.brand,
            payload.jato_model,
            payload.limit,
        )
    }


@router.post("/current-prices/{current_price_id}/remap")
def post_remap_current_price(
    current_price_id: str,
    payload: CurrentPriceRemapRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": remap_current_price(
            session,
            current_price_id,
            payload.model_dump(),
        )
    }


@router.get("/price-history")
def get_price_history(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    jato_trim: str | None = Query(default=None),
    jato_powertrain: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_price_history(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        limit,
    )
