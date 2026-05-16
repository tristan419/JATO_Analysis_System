from fastapi import APIRouter, Depends, HTTPException

from app.core.security import UserContext, require_min_role
from app.services.data_management_service import (
    read_airflow_ops_status,
    read_data_management_overview,
    read_voc_management_overview,
    start_airflow_stack,
    stop_airflow_stack,
)
from app.services.voc_staging_service import sync_voc_raw_to_store

router = APIRouter(prefix="/data-management", tags=["data-management"])


@router.get("/overview")
def get_data_management_overview(
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": read_data_management_overview()}


@router.get("/airflow/status")
def get_airflow_status(
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": read_airflow_ops_status()}


@router.post("/airflow/start")
def start_airflow(
    _user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    try:
        return {"item": start_airflow_stack()}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/airflow/stop")
def stop_airflow(
    _user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    try:
        return {"item": stop_airflow_stack()}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/voc/sync")
def sync_voc_raw(
    _user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    try:
        return {"item": sync_voc_raw_to_store()}
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/voc/overview")
def get_voc_overview(
    country: str | None = None,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": read_voc_management_overview(country)}
