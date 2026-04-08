from fastapi import APIRouter, Depends

from app.core.security import require_min_role
from app.services.query_service import metadata_columns

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/columns")
def columns(_=Depends(require_min_role("viewer"))) -> dict:
    return {"items": metadata_columns()}
