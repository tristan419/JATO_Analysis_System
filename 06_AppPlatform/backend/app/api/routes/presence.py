"""Presence API — Phase 1: REST heartbeat with auth-aware identity."""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel

from app.core.security import UserContext, get_optional_user
from app.services.presence_service import presence_store

router = APIRouter(prefix="/presence", tags=["presence"])


class HeartbeatPayload(BaseModel):
    session_id: str
    user_name: str = "anonymous"
    current_page: str = "unknown"


@router.post("/heartbeat")
def presence_heartbeat(
    payload: HeartbeatPayload | None = Body(None),
    user: UserContext = Depends(get_optional_user),
    page: str = Query("unknown", description="Current page (fallback)"),
) -> dict:
    """Register a heartbeat. Call every 30s from the frontend.

    User identity is resolved from X-Auth-Token header when available,
    falling back to the payload user_name for unauthenticated clients.
    """
    if payload:
        session_id = payload.session_id
        user_name = (
            user.name if user.name != "anonymous" else payload.user_name
        )
        current_page = payload.current_page
    else:
        session_id = user.name
        user_name = user.name
        current_page = page

    return presence_store.heartbeat(
        session_id=session_id,
        user_name=user_name,
        current_page=current_page,
        role=user.role,
    )


@router.get("/online")
def presence_online(
    page: str | None = Query(
        None, description="Filter same_page count by page"
    ),
) -> dict:
    """List currently online users, optionally scoped to a page."""
    return presence_store.get_online(current_page=page)
