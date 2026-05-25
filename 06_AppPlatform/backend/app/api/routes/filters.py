from fastapi import APIRouter, Depends

from app.api.schemas import FiltersOptionsRequest
from app.core.security import optional_viewer
from app.services.query_service import filters_options

router = APIRouter(prefix="/filters", tags=["filters"])


@router.post("/options")
def options(
    payload: FiltersOptionsRequest,
    _=Depends(optional_viewer),
) -> dict:
    return filters_options(payload.column, payload.filters)
