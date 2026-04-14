from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.schemas import CountryChatRequest
from app.core.security import require_min_role
from app.services.country_chat_service import (
    answer_country_question,
    get_country_chat_metadata,
)


router = APIRouter(prefix="/assistant/country", tags=["assistant"])


@router.get("/metadata")
def metadata(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return get_country_chat_metadata()


@router.post("/chat")
def chat(
    payload: CountryChatRequest,
    _=Depends(require_min_role("viewer")),
) -> dict:
    try:
        return answer_country_question(
            country=payload.country,
            question=payload.question,
            history=[turn.model_dump() for turn in payload.history],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
