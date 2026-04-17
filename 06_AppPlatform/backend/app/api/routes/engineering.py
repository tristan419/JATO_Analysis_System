from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.schemas import (
    ConfigImportRunRequest,
    ConfigProjectCreate,
    ConfigProjectPatch,
)
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.engineering_service import (
    archive_config_project,
    create_config_project,
    get_config_import_batch_detail,
    get_config_import_batch_page_data,
    list_config_import_batches,
    list_config_projects,
    list_config_variants,
    run_config_import,
    update_config_project,
)

router = APIRouter(prefix="/engineering/projects", tags=["engineering"])


@router.get("")
def get_projects(
    status: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    market_country: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_config_projects(session, status, brand, market_country, limit)


@router.get("/imports")
def get_config_imports(
    project_id: UUID | None = Query(default=None),
    import_status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_config_import_batches(
        session,
        project_id,
        import_status,
        limit,
    )


@router.get("/variants")
def get_config_variants(
    project_id: UUID | None = Query(default=None),
    config_import_batch_id: UUID | None = Query(default=None),
    model: str | None = Query(default=None),
    market_country: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_config_variants(
        session,
        project_id,
        config_import_batch_id,
        model,
        market_country,
        is_active,
        limit,
    )


@router.get("/imports/{config_import_batch_id}")
def get_config_import_detail(
    config_import_batch_id: str,
    sample_limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    row = get_config_import_batch_detail(
        session,
        config_import_batch_id,
        sample_limit,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Import batch not found")
    return {"item": row}


@router.get("/imports/{config_import_batch_id}/page-data")
def get_config_import_page_data(
    config_import_batch_id: str,
    sample_limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    row = get_config_import_batch_page_data(
        session,
        config_import_batch_id,
        sample_limit,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Import batch not found")
    return {"item": row}


@router.post("")
def post_project(
    payload: ConfigProjectCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": create_config_project(session, payload.model_dump())}


@router.post("/{project_id}/imports")
def post_config_import(
    project_id: str,
    payload: ConfigImportRunRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": run_config_import(
            session,
            project_id,
            payload.model_dump(),
        )
    }


@router.patch("/{project_id}")
def patch_project(
    project_id: str,
    payload: ConfigProjectPatch,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = update_config_project(
        session,
        project_id,
        payload.model_dump(),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return {"item": row}


@router.delete("/{project_id}")
def delete_project(
    project_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    row = archive_config_project(session, project_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return {"item": row}
