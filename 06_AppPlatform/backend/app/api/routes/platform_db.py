from fastapi import APIRouter, Depends

from app.core.security import require_min_role
from app.services.platform_db_service import read_database_health

router = APIRouter(prefix="/platform/db", tags=["platform-db"])


@router.get("/health")
def get_database_health(
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return read_database_health()
