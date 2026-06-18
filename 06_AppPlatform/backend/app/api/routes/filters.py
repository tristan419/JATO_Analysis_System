from fastapi import APIRouter, Depends

from app.api.schemas import FiltersOptionsBatchRequest, FiltersOptionsRequest
from app.core.security import optional_viewer
from app.services.query_service import filters_options, filters_options_batch

router = APIRouter(prefix="/filters", tags=["filters"])


@router.post("/options")
def options(
    payload: FiltersOptionsRequest,
    _=Depends(optional_viewer),
) -> dict:
    return filters_options(payload.column, payload.filters)


@router.post("/options/batch")
def options_batch(
    payload: FiltersOptionsBatchRequest,
    _=Depends(optional_viewer),
) -> dict:
    items = [(item.column, item.filters) for item in payload.items]
    return {"items": filters_options_batch(items)}
