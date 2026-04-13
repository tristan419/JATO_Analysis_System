from dataclasses import dataclass
from typing import Callable

from fastapi import Depends, Header, HTTPException

from app.core.config import AUTH_ENABLED, TOKEN_ROLE_MAP


ROLE_LEVEL = {
    "viewer": 1,
    "editor": 2,
    "admin": 3,
}


@dataclass(frozen=True)
class UserContext:
    role: str
    name: str


def get_current_user(
    x_auth_token: str | None = Header(default=None),
    x_user_name: str = Header(default="anonymous"),
) -> UserContext:
    if not AUTH_ENABLED:
        return UserContext(
            role="admin",
            name=str(x_user_name).strip() or "anonymous",
        )

    if not x_auth_token or x_auth_token not in TOKEN_ROLE_MAP:
        raise HTTPException(status_code=401, detail="Unauthorized")

    role = TOKEN_ROLE_MAP[x_auth_token]
    if role not in ROLE_LEVEL:
        raise HTTPException(status_code=403, detail="Invalid role")

    return UserContext(
        role=role,
        name=str(x_user_name).strip() or "anonymous",
    )


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
