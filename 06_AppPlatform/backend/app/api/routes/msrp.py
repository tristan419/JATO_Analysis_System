from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.schemas import (
    MsrpObservationCreate,
    MsrpObservationPatch,
    MsrpSourceCreate,
    MsrpSourcePatch,
)
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.msrp_admin_service import (
    create_observation,
    create_msrp_source,
    delete_observation,
    deactivate_msrp_source,
    list_observations,
    list_scrape_batches,
    list_msrp_sources,
    update_observation,
    update_msrp_source,
)

router = APIRouter(prefix="/msrp/sources", tags=["msrp"])


@router.get("")
def get_sources(
    source_code: str | None = Query(default=None),
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    enabled: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_msrp_sources(
        session,
        source_code,
        country,
        brand,
        source_type,
        enabled,
        limit,
    )


@router.get("/batches")
def get_scrape_batches(
    scope_country: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_scrape_batches(session, scope_country, status, limit)


@router.get("/observations")
def get_msrp_observations(
    scrape_batch_id: UUID | None = Query(default=None),
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    jato_model: str | None = Query(default=None),
    match_status: str | None = Query(default=None),
    source_code: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_observations(
        session,
        scrape_batch_id,
        country,
        brand,
        jato_model,
        match_status,
        source_code,
        source_type,
        limit,
    )


@router.post("/observations")
def post_msrp_observation(
    payload: MsrpObservationCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_observation(session, payload.model_dump())}


@router.patch("/observations/{observation_id}")
def patch_msrp_observation(
    observation_id: str,
    payload: MsrpObservationPatch,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = update_observation(
        session,
        observation_id,
        payload.model_dump(exclude_unset=True),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Observation not found")
    return {"item": row}


@router.delete("/observations/{observation_id}")
def delete_msrp_observation(
    observation_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = delete_observation(session, observation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Observation not found")
    return {"item": row}


@router.post("")
def post_source(
    payload: MsrpSourceCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_msrp_source(session, payload.model_dump())}


@router.patch("/{source_id}")
def patch_source(
    source_id: str,
    payload: MsrpSourcePatch,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = update_msrp_source(session, source_id, payload.model_dump())
    if row is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"item": row}


@router.delete("/{source_id}")
def delete_source(
    source_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = deactivate_msrp_source(session, source_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"item": row}
