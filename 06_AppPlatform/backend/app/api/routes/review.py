from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.schemas import MatchOverrideCreate, MatchOverridePatch
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.review_service import (
    create_match_override,
    list_match_overrides,
    update_match_override,
)

router = APIRouter(prefix="/review/overrides", tags=["review"])


@router.get("")
def get_overrides(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_match_overrides(session, country, brand, jato_model, limit)


@router.post("")
def post_override(
    payload: MatchOverrideCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_match_override(session, payload.model_dump())}


@router.patch("/{override_id}")
def patch_override(
    override_id: str,
    payload: MatchOverridePatch,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = update_match_override(session, override_id, payload.model_dump())
    if row is None:
        raise HTTPException(status_code=404, detail="Override not found")
    return {"item": row}
