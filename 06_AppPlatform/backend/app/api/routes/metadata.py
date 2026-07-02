from fastapi import APIRouter, Depends, Response

from app.api.cache_headers import set_strong_json_cache_headers
from app.core.security import optional_viewer
from app.services.query_service import metadata_columns, metadata_filter_snapshot

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/columns")
def columns(response: Response, _=Depends(optional_viewer)) -> dict:
    payload = {"items": metadata_columns()}
    set_strong_json_cache_headers(
        response,
        payload,
        namespace="metadata-columns",
    )
    return payload


@router.get("/filter-snapshot")
def filter_snapshot(response: Response, _=Depends(optional_viewer)) -> dict:
    payload = metadata_filter_snapshot()
    set_strong_json_cache_headers(
        response,
        payload,
        namespace="metadata-filter-snapshot",
    )
    return payload
