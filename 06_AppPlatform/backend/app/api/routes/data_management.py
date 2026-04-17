from fastapi import APIRouter, Depends

from app.core.security import UserContext, require_min_role
from app.services.data_management_service import read_data_management_overview

router = APIRouter(prefix="/data-management", tags=["data-management"])


@router.get("/overview")
def get_data_management_overview(
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": read_data_management_overview()}
