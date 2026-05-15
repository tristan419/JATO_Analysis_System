"""Auth routes — login, register, session."""

from __future__ import annotations

import secrets
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.config import FEISHU_ENABLED, FEISHU_REDIRECT_URI
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


class UpdateRoleBody(BaseModel):
    role: str


@router.patch("/users/{user_id}/role")
def update_user_role(
    user_id: str,
    body: UpdateRoleBody,
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Update a user's role. Admin only."""
    if body.role not in ("admin", "editor", "viewer"):
        raise HTTPException(status_code=400, detail="Invalid role")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = body.role
    db.commit()
    return {"id": str(user.id), "username": user.username, "role": user.role}


# ── Feishu OAuth ─────────────────────────────────────────────────


@router.get("/feishu/auth-url")
def feishu_auth_url(
    redirect: str = Query("/", description="Frontend page to return to"),
) -> dict:
    """Return the Feishu authorization URL."""
    if not FEISHU_ENABLED:
        raise HTTPException(status_code=503, detail="Feishu login not configured")
    state = secrets.token_urlsafe(16)
    url = _build_feishu_url(state, redirect)
    return {"url": url, "state": state}


@router.get("/feishu/callback")
def feishu_callback(
    code: str = Query(...),
    state: str = Query(...),
    redirect: str = Query("/"),
    db: Session = Depends(get_db_session),
) -> RedirectResponse:
    """Feishu OAuth callback — exchange code, find/create user, redirect with token."""
    if not FEISHU_ENABLED:
        raise HTTPException(status_code=503, detail="Feishu login not configured")

    from app.services.feishu_service import exchange_code

    try:
        user_info = exchange_code(code)
    except Exception as exc:
        raise HTTPException(
            status_code=401, detail=f"Feishu auth failed: {exc}"
        ) from exc

    feishu_name = str(user_info.get("name") or "feishu_user").strip()
    feishu_open_id = str(user_info.get("open_id") or "")

    if not feishu_open_id:
        raise HTTPException(status_code=401, detail="Missing Feishu open_id")

    username = f"feishu_{feishu_open_id[-8:]}"
    user = db.query(User).filter(User.username.like(f"feishu_{feishu_open_id[-8:]}")).first()

    if not user:
        from uuid import uuid4
        from app.services.auth_service import hash_password

        user = User(
            id=uuid4(),
            username=username,
            password_hash=hash_password(secrets.token_urlsafe(16)),
            role="viewer",
            is_active=True,
        )
        db.add(user)
        db.commit()
        db.refresh(user)

    token = session_store.create(user.username, user.role)

    # Redirect to frontend with token in URL fragment
    frontend_url = f"{redirect}?token={token}&username={user.username}&role={user.role}"
    return RedirectResponse(url=frontend_url)


def _build_feishu_url(state: str, redirect: str) -> str:
    from app.services.feishu_service import build_auth_url

    callback = (
        FEISHU_REDIRECT_URI
        + "?" + urlencode({"state": state, "redirect": redirect})
    )
    return build_auth_url(state=state, redirect_uri=callback)
