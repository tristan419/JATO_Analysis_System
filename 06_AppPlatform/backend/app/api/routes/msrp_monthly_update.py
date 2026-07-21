from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from app.core.security import UserContext, require_min_role
from app.services.jato_monthly_update_service import (
    abandon_jato_monthly_update_upload,
    approve_jato_monthly_update_review,
    cancel_jato_monthly_update_job,
    complete_jato_monthly_update_upload,
    create_jato_monthly_update_job,
    create_jato_monthly_update_job_from_upload,
    create_single_country_job,
    create_smart_merge_candidate,
    get_jato_monthly_update_upload,
    get_jato_monthly_update_job,
    get_jato_monthly_update_maintenance_status,
    get_jato_monthly_update_review,
    get_jato_monthly_update_expected_chunk_size,
    initiate_jato_monthly_update_upload,
    list_jato_monthly_update_jobs,
    promote_current_active_to_baseline,
    publish_jato_monthly_update_job,
    recheck_jato_monthly_update_job,
    recover_failed_jato_monthly_update_job,
    refresh_jato_monthly_update_review,
    resume_failed_jato_smart_merge,
    resolve_jato_historical_reclassification,
    retry_jato_monthly_update_upload_digest,
    rollback_jato_monthly_update_job,
    retry_failed_jato_monthly_update_job,
    run_jato_monthly_update_cleanup,
    upload_jato_monthly_update_chunk,
)

router = APIRouter(prefix="/msrp", tags=["msrp"])


class JatoMonthlyUpdateReviewRefreshBody(BaseModel):
    requestId: str = Field(min_length=8, max_length=128)
    expectedCandidateFingerprint: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )


class JatoSmartMergeResumeBody(BaseModel):
    requestId: str = Field(
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$",
    )
    expectedSourceCandidateFingerprint: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )
    expectedActiveFingerprint: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )
    expectedReportFingerprint: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )
    expectedResolutionFingerprint: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )


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
    month_raw = payload.get("month")
    return {
        "item": create_jato_monthly_update_job_from_upload(
            upload_id=str(payload.get("uploadId", "")),
            triggered_by=user.name,
            triggered_role=user.role,
            month=str(month_raw).strip() if month_raw else None,
        )
    }


@router.post("/monthly-update-jobs/single-country")
def post_single_country_job(
    country: str = Form(...),
    month: str = Form(...),
    file: UploadFile = File(...),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": create_single_country_job(
            country=country,
            month=month,
            file=file,
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


@router.post("/monthly-update-jobs/{job_id}/recover")
def post_recover_monthly_update_job(
    job_id: str,
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": recover_failed_jato_monthly_update_job(
            source_job_id=job_id,
            recovery_key=str(payload.get("recoveryKey") or ""),
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/{job_id}/recheck")
def post_recheck_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": recheck_jato_monthly_update_job(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/{job_id}/cancel")
def post_cancel_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": cancel_jato_monthly_update_job(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.get("/monthly-update-jobs/{job_id}/review")
def get_monthly_update_review(
    job_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {"item": get_jato_monthly_update_review(job_id)}


@router.post("/monthly-update-jobs/{job_id}/review-refresh")
def post_monthly_update_review_refresh(
    job_id: str,
    payload: JatoMonthlyUpdateReviewRefreshBody,
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": refresh_jato_monthly_update_review(
            job_id=job_id,
            triggered_by=user.name,
            request_id=payload.requestId,
            expected_candidate_fingerprint=(
                payload.expectedCandidateFingerprint
            ),
        )
    }


@router.post("/monthly-update-jobs/{job_id}/review-approval")
def post_monthly_update_review_approval(
    job_id: str,
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": approve_jato_monthly_update_review(
            job_id=job_id,
            triggered_by=user.name,
            decision=str(payload.get("decision", "")),
            note=str(payload.get("note", "")) if payload.get("note") is not None else None,
        )
    }


@router.post(
    "/monthly-update-jobs/{job_id}/historical-reclassification-resolution"
)
def post_historical_reclassification_resolution(
    job_id: str,
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": resolve_jato_historical_reclassification(
            job_id=job_id,
            triggered_by=user.name,
            decisions=payload.get("decisions"),
        )
    }


@router.post("/monthly-update-jobs/{job_id}/publish")
def post_publish_monthly_update_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("admin")),
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
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": rollback_jato_monthly_update_job(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/{job_id}/smart-merge")
def post_smart_merge_candidate(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": create_smart_merge_candidate(
            job_id=job_id,
            triggered_by=user.name,
        )
    }


@router.post("/monthly-update-jobs/{job_id}/smart-merge-resume")
def post_smart_merge_resume(
    job_id: str,
    payload: JatoSmartMergeResumeBody,
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": resume_failed_jato_smart_merge(
            job_id=job_id,
            triggered_by=user.name,
            request_id=payload.requestId,
            expected_source_candidate_fingerprint=(
                payload.expectedSourceCandidateFingerprint
            ),
            expected_active_fingerprint=(
                payload.expectedActiveFingerprint
            ),
            expected_report_fingerprint=(
                payload.expectedReportFingerprint
            ),
            expected_resolution_fingerprint=(
                payload.expectedResolutionFingerprint
            ),
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
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": get_jato_monthly_update_upload(
            upload_id,
            requested_by=user.name,
            requested_role=user.role,
        )
    }


@router.put("/monthly-update-uploads/{upload_id}/parts/{part_number}")
async def put_monthly_update_upload_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    expected_size = get_jato_monthly_update_expected_chunk_size(
        upload_id=upload_id,
        part_number=part_number,
        requested_by=user.name,
        requested_role=user.role,
    )
    content_length = request.headers.get("Content-Length")
    if content_length:
        try:
            declared_size = int(content_length)
        except ValueError:
            raise HTTPException(status_code=400, detail="Content-Length 无效。") from None
        if declared_size > expected_size:
            raise HTTPException(status_code=413, detail="上传分片超过该会话允许的大小。")
    content = bytearray()
    async for block in request.stream():
        content.extend(block)
        if len(content) > expected_size:
            raise HTTPException(status_code=413, detail="上传分片超过该会话允许的大小。")
    return {
        "item": upload_jato_monthly_update_chunk(
            upload_id=upload_id,
            part_number=part_number,
            content=bytes(content),
            chunk_sha256=request.headers.get("X-Chunk-SHA256", ""),
            requested_by=user.name,
            requested_role=user.role,
        )
    }


@router.post("/monthly-update-uploads/{upload_id}/complete")
def post_monthly_update_upload_complete(
    upload_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by=user.name,
            requested_role=user.role,
        )
    }


@router.post("/monthly-update-uploads/{upload_id}/retry-digest")
def post_monthly_update_upload_retry_digest(
    upload_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": retry_jato_monthly_update_upload_digest(
            upload_id=upload_id,
            requested_by=user.name,
            requested_role=user.role,
        )
    }


@router.post("/monthly-update-uploads/{upload_id}/abandon")
def post_monthly_update_upload_abandon(
    upload_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": abandon_jato_monthly_update_upload(
            upload_id=upload_id,
            triggered_by=user.name,
            triggered_role=user.role,
        )
    }


@router.post("/monthly-update-maintenance/cleanup")
def post_monthly_update_cleanup(
    payload: dict[str, object] | None = None,
    user: UserContext = Depends(require_min_role("admin")),
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
    user: UserContext = Depends(require_min_role("admin")),
) -> dict[str, object]:
    return {
        "item": promote_current_active_to_baseline(triggered_by=user.name)
    }
