"""Presence API — Phase 1: REST heartbeat.

Lightweight user presence tracking. No WebSocket yet (Phase 2).
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Header, Query

router = APIRouter(prefix="/presence", tags=["presence"])

# In-memory store: {user_id: {"lastSeen": timestamp, "page": str}}
_store: dict[str, dict[str, Any]] = {}
HEARTBEAT_TTL_SECONDS = 60


def _clean_stale() -> None:
    """Remove users who haven't sent a heartbeat in TTL seconds."""
    now = time.time()
    stale = [uid for uid, v in _store.items() if now - v["lastSeen"] > HEARTBEAT_TTL_SECONDS]
    for uid in stale:
        del _store[uid]


@router.post("/heartbeat")
def presence_heartbeat(
    page: str = Query("unknown", description="Current page/route the user is on"),
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
) -> dict:
    """Register a heartbeat. Call every 30s from the frontend."""
    user = x_user_name.strip() or "anonymous"
    _store[user] = {"lastSeen": time.time(), "page": page, "user": user}
    _clean_stale()
    return {"status": "ok", "online": len(_store)}


@router.get("/online")
def presence_online() -> dict:
    """List currently online users."""
    _clean_stale()
    users = [
        {"user": v["user"], "page": v["page"], "lastSeen": v["lastSeen"]}
        for v in _store.values()
    ]
    return {"online": len(users), "users": sorted(users, key=lambda u: u["user"])}
