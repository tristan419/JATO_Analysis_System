"""COC match router — endpoints for file upload, job management, and report download."""

import re

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, Response

from app.core.security import UserContext, require_min_role
from app.services.coc_fill_service import (
    apply_coc_fill_overrides,
    complete_coc_fill_upload,
    create_coc_fill_job,
    create_coc_fill_job_from_upload,
    get_coc_fill_job,
    get_coc_fill_workbook_path,
    initiate_coc_fill_upload,
    list_coc_fill_jobs,
    revert_coc_fill_overrides,
    upload_coc_fill_chunk,
)
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


def _parse_sheet_names(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        names = [str(item).strip() for item in value if str(item).strip()]
        return names or None
    names = [item.strip() for item in re.split(r"[,，\n\r]+", str(value)) if item.strip()]
    return names or None


@router.post("/fill/jobs")
async def post_coc_fill_job(
    excel: UploadFile = File(...),
    pdf: UploadFile = File(...),
    overwrite_existing: bool = Form(default=False),
    conflict_strategy: str = Form(default="strict"),
    include_result_sheet: bool = Form(default=False),
    sheet_names: str | None = Form(default=None),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Create a COC fill job with a shipment workbook and WVTA relation PDF."""
    return {
        "item": create_coc_fill_job(
            excel_file=excel,
            pdf_file=pdf,
            overwrite_existing=overwrite_existing,
            conflict_strategy=conflict_strategy,
            include_result_sheet=include_result_sheet,
            triggered_by=user.name,
            sheet_names=_parse_sheet_names(sheet_names),
        )
    }


@router.post("/fill/jobs/batch")
def post_coc_fill_job_batch(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Create a COC fill job from two completed chunked uploads."""
    return {
        "item": create_coc_fill_job_from_upload(
            excel_upload_id=str(payload.get("excelUploadId", "")),
            pdf_upload_id=str(payload.get("pdfUploadId", "")),
            excel_filename=str(payload.get("excelFilename", "")),
            pdf_filename=str(payload.get("pdfFilename", "")),
            overwrite_existing=bool(payload.get("overwriteExisting", False)),
            conflict_strategy=str(payload.get("conflictStrategy", "strict")),
            triggered_by=user.name,
            include_result_sheet=bool(payload.get("includeResultSheet", False)),
            sheet_names=_parse_sheet_names(payload.get("sheetNames")),
        )
    }


@router.post("/fill/upload-sessions/initiate")
def post_coc_fill_upload_session(
    payload: dict[str, object],
    user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Initiate a chunked upload session for a COC fill source file."""
    return {
        "item": initiate_coc_fill_upload(
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


@router.put("/fill/upload-sessions/{upload_id}/parts/{part_number}")
async def put_coc_fill_upload_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Upload a single chunk for a COC fill source file."""
    return {
        "item": upload_coc_fill_chunk(
            upload_id=upload_id,
            part_number=part_number,
            content=await request.body(),
            chunk_sha256=request.headers.get("X-Chunk-SHA256", ""),
        )
    }


@router.post("/fill/upload-sessions/{upload_id}/complete")
def post_coc_fill_upload_complete(
    upload_id: str,
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Mark a COC fill chunked upload session as complete."""
    return {"item": complete_coc_fill_upload(upload_id)}


@router.get("/fill/jobs")
def get_coc_fill_jobs(
    limit: int = Query(default=50, ge=1, le=50),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    """List COC fill jobs, most recent first."""
    return list_coc_fill_jobs(limit=limit)


@router.get("/fill/jobs/{job_id}")
def get_coc_fill_job_detail(
    job_id: str,
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    """Get COC fill job status and summary."""
    return {"item": get_coc_fill_job(job_id)}


@router.post("/fill/jobs/{job_id}/overrides")
def post_coc_fill_overrides(
    job_id: str,
    payload: dict[str, object],
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Apply manually selected WVTA/COC candidates to an existing fill job."""
    overrides = payload.get("overrides")
    if not isinstance(overrides, list):
        overrides = []
    return {
        "item": apply_coc_fill_overrides(
            job_id,
            [item for item in overrides if isinstance(item, dict)],
        )
    }


@router.post("/fill/jobs/{job_id}/overrides/revert")
def post_coc_fill_override_revert(
    job_id: str,
    payload: dict[str, object],
    _user: UserContext = Depends(require_min_role("editor")),
) -> dict[str, object]:
    """Revert manually selected WVTA/COC candidates on an existing fill job."""
    overrides = payload.get("overrides")
    if not isinstance(overrides, list):
        overrides = []
    return {
        "item": revert_coc_fill_overrides(
            job_id,
            [item for item in overrides if isinstance(item, dict)],
        )
    }


@router.get("/fill/jobs/{job_id}/workbook")
def get_coc_fill_workbook(
    job_id: str,
    _user: UserContext = Depends(require_min_role("viewer")),
) -> FileResponse:
    """Download the filled COC workbook."""
    workbook_path = get_coc_fill_workbook_path(job_id)
    job = get_coc_fill_job(job_id)
    return FileResponse(
        workbook_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=str(job.get("excelFilename") or f"coc_fill_{job_id}.xlsx"),
    )


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
    country: str | None = Query(default=None),
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    """List COC match jobs, most recent first."""
    return list_coc_match_jobs(limit=limit, country=country)


@router.get("/jobs/{job_id}")
def get_coc_match_job_detail(
    job_id: str,
    _user: UserContext = Depends(require_min_role("viewer")),
) -> dict[str, object]:
    """Get COC match job status and results."""
    return {"item": get_coc_match_job(job_id)}


@router.get("/jobs/{job_id}/report")
def get_coc_match_report(
    job_id: str,
    download: bool = Query(default=False),
    _user: UserContext = Depends(require_min_role("viewer")),
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
