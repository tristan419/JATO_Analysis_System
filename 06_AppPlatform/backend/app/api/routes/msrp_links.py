from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.msrp_schemas import JatoMsrpLinkCreate, JatoMsrpLinkPatch
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.msrp_admin_service import (
    create_jato_msrp_link,
    deactivate_jato_msrp_link,
    list_jato_msrp_links,
    update_jato_msrp_link,
)

router = APIRouter(prefix="/msrp/links", tags=["msrp"])


@router.get("")
def get_jato_msrp_links(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    official_model: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_jato_msrp_links(
        session,
        country,
        brand,
        jato_model,
        official_model,
        is_active,
        limit,
    )


@router.post("")
def post_jato_msrp_link(
    payload: JatoMsrpLinkCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_jato_msrp_link(session, payload.model_dump())}


@router.patch("/{link_id}")
def patch_jato_msrp_link(
    link_id: str,
    payload: JatoMsrpLinkPatch,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = update_jato_msrp_link(
        session,
        link_id,
        payload.model_dump(exclude_unset=True),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"item": row}


@router.delete("/{link_id}")
def delete_jato_msrp_link(
    link_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = deactivate_jato_msrp_link(session, link_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"item": row}
