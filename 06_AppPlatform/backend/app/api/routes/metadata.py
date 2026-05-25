from fastapi import APIRouter, Depends

from app.core.security import optional_viewer
from app.services.query_service import metadata_columns

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/columns")
def columns(_=Depends(optional_viewer)) -> dict:
    return {"items": metadata_columns()}
