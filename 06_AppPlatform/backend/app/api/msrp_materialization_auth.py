from __future__ import annotations

from dataclasses import dataclass

from fastapi import Header, HTTPException

from app.services.auth_service import session_store


@dataclass(frozen=True, slots=True)
class MaterializationActor:
    role: str
    name: str
    identity_source: str = "authenticated_session"


def require_materialization_editor_session(
    x_auth_token: str | None = Header(default=None),
) -> MaterializationActor:
    """Reject static tokens and header-derived dev identities for fact writes."""

    authenticated = session_store.lookup(x_auth_token) if x_auth_token else None
    if authenticated is None or authenticated.role not in {"editor", "admin"}:
        raise HTTPException(
            status_code=403,
            detail=(
                "MSRP fact writes require an authenticated editor/admin login "
                "session; static service tokens are not accepted."
            ),
        )
    return MaterializationActor(
        role=authenticated.role,
        name=authenticated.username,
    )
