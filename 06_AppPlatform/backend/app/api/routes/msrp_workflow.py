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
    create_scrape_batch_ingest,
    list_current_prices,
    list_price_history,
    materialize_current_prices,
    remap_current_price,
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
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_price_history(
        session, country, brand, jato_model, jato_trim, limit
    )
