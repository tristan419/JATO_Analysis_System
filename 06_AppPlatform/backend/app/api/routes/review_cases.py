from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.schemas import ReviewDecisionCreate
from app.core.security import require_min_role
from app.db.session import get_db_session
from app.services.review_service import (
    create_review_decision,
    get_review_case_detail,
    list_review_cases,
)
from app.services.review_workbench_service import get_review_workbench
from app.infra.review_repository import count_distinct_countries
from app.services.country_service import JATO_BASELINE_COUNTRY_COUNT

router = APIRouter(prefix="/review/cases", tags=["review"])


@router.get("")
def get_cases(
    review_status: str | None = Query(default=None),
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    current_assignee: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return list_review_cases(
        session,
        review_status,
        country,
        brand,
        current_assignee,
        limit,
        offset,
        model,
    )


@router.get("/stats")
def get_cases_stats(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return {
        "totalCountries": count_distinct_countries(session),
        "jatoCountries": JATO_BASELINE_COUNTRY_COUNT,
    }


@router.get("/workbench")
def get_cases_workbench(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    return {"item": get_review_workbench(country, brand)}


@router.get("/{review_case_id}")
def get_case_detail(
    review_case_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict[str, object]:
    row = get_review_case_detail(session, review_case_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Review case not found")
    return {"item": row}


@router.post("/{review_case_id}/decisions")
def post_case_decision(
    review_case_id: str,
    payload: ReviewDecisionCreate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict[str, object]:
    return {
        "item": create_review_decision(
            session,
            review_case_id,
            payload.model_dump(),
        )
    }
