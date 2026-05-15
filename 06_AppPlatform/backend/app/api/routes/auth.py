"""Auth routes — login, register, session."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.security import UserContext, get_current_user, require_min_role
from app.db.models import User
from app.db.session import get_db_session
from app.services.auth_service import (
    authenticate,
    create_user,
    list_users,
    session_store,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginBody(BaseModel):
    username: str
    password: str


class RegisterBody(BaseModel):
    username: str
    password: str
    role: str = "viewer"


class LoginResponse(BaseModel):
    token: str
    username: str
    role: str


@router.post("/login")
def login(
    body: LoginBody, db: Session = Depends(get_db_session)
) -> LoginResponse:
    token = authenticate(db, body.username.strip(), body.password)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user = (
        db.query(User)
        .filter(User.username == body.username.strip())
        .first()
    )
    return LoginResponse(
        token=token,
        username=user.username,
        role=user.role,
    )


@router.post("/register")
def register(
    body: RegisterBody,
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Create a new user. Admin only."""
    username = body.username.strip()
    if not username or len(username) < 2:
        raise HTTPException(status_code=400, detail="Username too short")
    if len(body.password) < 6:
        raise HTTPException(status_code=400, detail="Password too short (min 6)")
    if body.role not in ("admin", "editor", "viewer"):
        raise HTTPException(status_code=400, detail="Invalid role")

    try:
        user = create_user(db, username, body.password, body.role)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return {
        "id": str(user.id),
        "username": user.username,
        "role": user.role,
    }


@router.get("/me")
def me(user: UserContext = Depends(get_current_user)) -> dict:
    """Return the current authenticated user."""
    return {"username": user.name, "role": user.role}


@router.post("/logout")
def logout(
    x_auth_token: str | None = Header(None, alias="X-Auth-Token"),
    _: UserContext = Depends(get_current_user),
) -> dict:
    """Revoke the current session token."""
    if x_auth_token:
        session_store.revoke(x_auth_token)
    return {"status": "ok"}


@router.get("/users")
def users_list(
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """List all users. Admin only."""
    return {"users": list_users(db)}
