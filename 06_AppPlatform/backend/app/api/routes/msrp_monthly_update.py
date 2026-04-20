from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile

from app.core.security import UserContext, require_min_role
from app.services.jato_monthly_update_service import (
    complete_jato_monthly_update_upload,
    create_jato_monthly_update_job,
    create_jato_monthly_update_job_from_upload,
    get_jato_monthly_update_upload,
    get_jato_monthly_update_job,
    get_jato_monthly_update_maintenance_status,
    get_jato_monthly_update_review,
    initiate_jato_monthly_update_upload,
    list_jato_monthly_update_jobs,
    promote_current_active_to_baseline,
    publish_jato_monthly_update_job,
    rollback_jato_monthly_update_job,
    retry_failed_jato_monthly_update_job,
    run_jato_monthly_update_cleanup,
    upload_jato_monthly_update_chunk,
)

router = APIRouter(prefix="/msrp", tags=["msrp"])


@router.post("/monthly-update-jobs")
def post_monthly_update_job(
    month: str | None = Form(default=None),
    file: UploadFile = File(...),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    _ = month
    return {
        "item": create_jato_monthly_update_job(
            file=file,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/from-upload")
def post_monthly_update_job_from_upload(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": create_jato_monthly_update_job_from_upload(
            upload_id=str(payload.get("uploadId", "")),
            triggered_by=user.name,
        )
    }


@router.get("/monthly-update-jobs")
def get_monthly_update_jobs(
    limit: int = Query(default=20, ge=1, le=50),
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return list_jato_monthly_update_jobs(limit=limit)


@router.get("/monthly-update-jobs/{job_id}")
def get_monthly_update_job(
    job_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": get_jato_monthly_update_job(job_id)}


@router.post("/monthly-update-jobs/{job_id}/retry")
def post_retry_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": retry_failed_jato_monthly_update_job(
            source_job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.get("/monthly-update-jobs/{job_id}/review")
def get_monthly_update_review(
    job_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": get_jato_monthly_update_review(job_id)}


@router.post("/monthly-update-jobs/{job_id}/publish")
def post_publish_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": publish_jato_monthly_update_job(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/{job_id}/rollback")
def post_rollback_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": rollback_jato_monthly_update_job(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-uploads/initiate")
def post_monthly_update_upload_session(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": initiate_jato_monthly_update_upload(
            filename=str(payload.get("filename", "")),
            size_bytes=payload.get("sizeBytes"),
            resume_key=(
                str(payload.get("resumeKey", "")).strip()
                if payload.get("resumeKey") is not None
                else None
            ),
            triggered_by=user.name,
        )
    }


@router.get("/monthly-update-uploads/{upload_id}")
def get_monthly_update_upload_session(
    upload_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": get_jato_monthly_update_upload(upload_id)}


@router.put("/monthly-update-uploads/{upload_id}/parts/{part_number}")
async def put_monthly_update_upload_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": upload_jato_monthly_update_chunk(
            upload_id=upload_id,
            part_number=part_number,
            content=await request.body(),
            chunk_sha256=request.headers.get("X-Chunk-SHA256", ""),
        )
    }


@router.post("/monthly-update-uploads/{upload_id}/complete")
def post_monthly_update_upload_complete(
    upload_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": complete_jato_monthly_update_upload(upload_id=upload_id)}


@router.post("/monthly-update-maintenance/cleanup")
def post_monthly_update_cleanup(
    payload: dict[str, object] | None = None,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": run_jato_monthly_update_cleanup(
            triggered_by=user.name,
            cleanup_tier=str((payload or {}).get("cleanupTier", "safe")),
        )
    }


@router.get("/monthly-update-maintenance/status")
def get_monthly_update_maintenance_status(
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": get_jato_monthly_update_maintenance_status()}


@router.post("/monthly-update-maintenance/promote-baseline")
def post_monthly_update_promote_baseline(
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": promote_current_active_to_baseline(triggered_by=user.name)
    }
