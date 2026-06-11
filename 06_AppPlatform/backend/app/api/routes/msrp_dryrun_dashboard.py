"""API routes for MSRP dryrun progress dashboard."""

from fastapi import APIRouter, Depends, Query

from app.core.security import require_min_role
from app.services.msrp_dryrun_progress import (
    get_dryrun_country_detail,
    get_dryrun_dashboard,
)

router = APIRouter(prefix="/msrp-dryrun", tags=["msrp-dryrun"])


@router.get("/dashboard")
def dryrun_dashboard(
    run_id: str | None = Query(None),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Live progress + historical runs for the MSRP dryrun pipeline."""
    return get_dryrun_dashboard(run_id=run_id)


@router.get("/country-detail")
def dryrun_country_detail(
    log_file: str = Query(...),
    country_code: str = Query(...),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Per-source results for a specific country from a specific log file."""
    return get_dryrun_country_detail(log_file, country_code)
