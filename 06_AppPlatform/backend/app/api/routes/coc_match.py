"""COC match router — endpoints for file upload, job management, and report download."""

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, Response

from app.core.security import UserContext, require_min_role
from app.services.coc_match_service import (
    complete_coc_match_upload,
    create_coc_match_job,
    create_coc_match_job_from_upload,
    get_coc_match_job,
    get_coc_match_report_path,
    initiate_coc_match_upload,
    list_coc_match_jobs,
    retry_failed_coc_match_job,
    upload_coc_match_chunk,
)

router = APIRouter(prefix="/coc-match", tags=["coc-match"])


@router.post("/jobs")
async def post_coc_match_job(
    excel: UploadFile = File(...),
    archive: UploadFile = File(...),
    country: str = Form(...),
    month: str | None = Form(default=None),
    file_ext: str = Form(default=".pdf"),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Create a COC match job with two uploaded files (Excel + ZIP/RAR).
    Use this for small files (< 50 MB). For larger files use chunked upload
    sessions + /jobs/from-upload.
    """
    return {
        "item": create_coc_match_job(
            excel_file=excel,
            archive_file=archive,
            country=country,
            month=month,
            file_ext=file_ext,
            triggered_by=user.name,
        )
    }


@router.post("/jobs/batch")
def post_coc_match_job_batch(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Create a COC match job from two completed chunked uploads.
    Payload: { excelUploadId, archiveUploadId, excelFilename, archiveFilename,
               country, month?, fileExt }
    """
    return {
        "item": create_coc_match_job_from_upload(
            excel_upload_id=str(payload.get("excelUploadId", "")),
            archive_upload_id=str(payload.get("archiveUploadId", "")),
            excel_filename=str(payload.get("excelFilename", "")),
            archive_filename=str(payload.get("archiveFilename", "")),
            country=str(payload.get("country", "")),
            month=str(payload.get("month")) if payload.get("month") else None,
            file_ext=str(payload.get("fileExt", ".pdf")),
            triggered_by=user.name,
        )
    }


@router.post("/upload-sessions/initiate")
def post_coc_upload_session(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Initiate a chunked upload session for a large COC file."""
    return {
        "item": initiate_coc_match_upload(
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


@router.put("/upload-sessions/{upload_id}/parts/{part_number}")
async def put_coc_upload_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Upload a single chunk for a COC file upload session."""
    return {
        "item": upload_coc_match_chunk(
            upload_id=upload_id,
            part_number=part_number,
            content=await request.body(),
            chunk_sha256=request.headers.get("X-Chunk-SHA256", ""),
        )
    }


@router.post("/upload-sessions/{upload_id}/complete")
def post_coc_upload_complete(
    upload_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Mark a chunked upload session as complete and assemble the file."""
    return {"item": complete_coc_match_upload(upload_id)}


@router.get("/jobs")
def get_coc_match_jobs(
    limit: int = Query(default=20, ge=1, le=50),
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """List COC match jobs, most recent first."""
    return list_coc_match_jobs(limit=limit)


@router.get("/jobs/{job_id}")
def get_coc_match_job_detail(
    job_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Get COC match job status and results."""
    return {"item": get_coc_match_job(job_id)}


@router.get("/jobs/{job_id}/report")
def get_coc_match_report(
    job_id: str,
    download: bool = Query(default=False),
    _user: UserContext = Depends(require_min_role("editor")),
) -> Response:
    """Get COC match HTML report. Add ?download=1 to download as file."""
    report_path = get_coc_match_report_path(job_id)
    content = report_path.read_text(encoding="utf-8")
    if download:
        return Response(
            content=content,
            media_type="text/html",
            headers={"Content-Disposition": f'attachment; filename="coc_report_{job_id}.html"'},
        )
    return HTMLResponse(content=content)


@router.post("/jobs/{job_id}/retry")
def post_retry_coc_match_job(
    job_id: str,
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Retry a failed COC match job using the original files."""
    return {
        "item": retry_failed_coc_match_job(
            source_job_id=job_id,
            triggered_by=user.name,
        )
    }
