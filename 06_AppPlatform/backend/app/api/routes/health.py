from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.jato_monthly_update_service import jato_monthly_availability
from app.services.readiness_service import build_readiness_report

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
def readyz() -> JSONResponse:
    report = build_readiness_report()
    monthly = jato_monthly_availability()
    report["monthlyUpdate"] = {
        "enabled": monthly["enabled"],
        "reason": monthly["reason"],
    }
    status_code = 200 if report.get("status") == "ready" else 503
    return JSONResponse(
        status_code=status_code,
        content=report,
        headers={"Cache-Control": "no-store"},
    )
