from dataclasses import dataclass
from typing import Callable

from fastapi import Depends, Header, HTTPException

from app.core.config import AUTH_ENABLED, TOKEN_ROLE_MAP
from app.services.auth_service import session_store


ROLE_LEVEL = {
    "viewer": 1,
    "editor": 2,
    "admin": 3,
}


@dataclass(frozen=True)
class UserContext:
    role: str
    name: str


def _anonymous_viewer(x_user_name: str = "anonymous") -> UserContext:
    return UserContext(
        role="viewer",
        name=str(x_user_name).strip() or "anonymous",
    )


def _token_user(
    x_auth_token: str | None,
    x_user_name: str = "anonymous",
) -> UserContext | None:
    if not x_auth_token:
        return None

    session = session_store.lookup(x_auth_token)
    if session and session.role in ROLE_LEVEL:
        return UserContext(role=session.role, name=session.username)

    if x_auth_token in TOKEN_ROLE_MAP:
        role = TOKEN_ROLE_MAP[x_auth_token]
        if role not in ROLE_LEVEL:
            raise HTTPException(status_code=403, detail="Invalid role")
        return UserContext(
            role=role,
            name=str(x_user_name).strip() or "anonymous",
        )

    return None


def get_current_user(
    x_auth_token: str | None = Header(default=None),
    x_user_name: str = Header(default="anonymous"),
) -> UserContext:
    if not AUTH_ENABLED:
        return UserContext(
            role="admin",
            name=str(x_user_name).strip() or "anonymous",
        )

    return _token_user(x_auth_token, x_user_name) or _anonymous_viewer(x_user_name)


def require_min_role(min_role: str) -> Callable:
    min_level = ROLE_LEVEL.get(min_role, ROLE_LEVEL["viewer"])

    def dependency(
        user: UserContext = Depends(get_current_user),
    ) -> UserContext:
        current_level = ROLE_LEVEL.get(user.role, 0)
        if current_level < min_level:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return dependency


def get_optional_user(
    x_auth_token: str | None = Header(default=None),
    x_user_name: str = Header(default="anonymous"),
) -> UserContext:
    name = str(x_user_name).strip() or "anonymous"

    if not AUTH_ENABLED:
        return UserContext(role="admin", name=name)

    return _token_user(x_auth_token, name) or _anonymous_viewer(name)


def optional_viewer(user: UserContext = Depends(get_optional_user)) -> UserContext:
    """Allow anonymous read access while preserving authenticated user context."""
    return user
