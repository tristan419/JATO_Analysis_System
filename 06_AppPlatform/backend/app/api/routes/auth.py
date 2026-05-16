"""Auth routes — login, register, session."""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.config import (
    FEISHU_ENABLED,
    FEISHU_REDIRECT_URI,
    GOOGLE_ENABLED,
    GOOGLE_REDIRECT_URI,
)
from app.core.security import (
    ROLE_LEVEL,
    UserContext,
    get_current_user,
    require_min_role,
)
from app.db.models import RoleUpgradeRequest, User
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


# ── Role Upgrade Requests ────────────────────────────────────────


class RoleUpgradeRequestBody(BaseModel):
    requested_role: str
    reason: str = ""


class ReviewUpgradeBody(BaseModel):
    status: str  # "approved" or "rejected"


@router.post("/role-upgrade/request")
def request_role_upgrade(
    body: RoleUpgradeRequestBody,
    db: Session = Depends(get_db_session),
    user: UserContext = Depends(get_current_user),
) -> dict:
    if body.requested_role not in ("editor", "admin"):
        raise HTTPException(status_code=400, detail="Invalid requested role")
    if ROLE_LEVEL.get(body.requested_role, 0) <= ROLE_LEVEL.get(user.role, 0):
        raise HTTPException(status_code=400, detail="Cannot downgrade or request same level")

    existing = (
        db.query(RoleUpgradeRequest)
        .filter(
            RoleUpgradeRequest.user_id == user.name,  # username as join
            RoleUpgradeRequest.status == "pending",
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="You already have a pending request")

    db_user = db.query(User).filter(User.username == user.name).first()
    req = RoleUpgradeRequest(
        user_id=db_user.id if db_user else None,
        username=user.name,
        current_role=user.role,
        requested_role=body.requested_role,
        reason=body.reason,
        status="pending",
    )
    db.add(req)
    db.commit()
    return {
        "requestId": str(req.request_id),
        "username": req.username,
        "requestedRole": req.requested_role,
        "status": req.status,
    }


@router.get("/role-upgrade/requests")
def list_role_upgrade_requests(
    status: str | None = Query(None),
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    q = db.query(RoleUpgradeRequest).order_by(RoleUpgradeRequest.created_at_utc.desc())
    if status:
        q = q.filter(RoleUpgradeRequest.status == status)
    items = q.limit(50).all()
    return {
        "requests": [
            {
                "requestId": str(r.request_id),
                "username": r.username,
                "currentRole": r.current_role,
                "requestedRole": r.requested_role,
                "reason": r.reason,
                "status": r.status,
                "createdAtUtc": r.created_at_utc.isoformat(),
            }
            for r in items
        ]
    }


@router.patch("/role-upgrade/requests/{request_id}")
def review_role_upgrade(
    request_id: str,
    body: ReviewUpgradeBody,
    db: Session = Depends(get_db_session),
    admin: UserContext = Depends(require_min_role("admin")),
) -> dict:
    if body.status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail="Status must be approved or rejected")

    req = db.query(RoleUpgradeRequest).filter(
        RoleUpgradeRequest.request_id == request_id
    ).first()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if req.status != "pending":
        raise HTTPException(status_code=409, detail="Request already reviewed")

    req.status = body.status
    req.reviewed_by = admin.name
    req.reviewed_at_utc = datetime.now(timezone.utc)

    if body.status == "approved":
        db_user = db.query(User).filter(User.username == req.username).first()
        if db_user:
            db_user.role = req.requested_role

    db.commit()
    return {
        "requestId": str(req.request_id),
        "status": req.status,
        "username": req.username,
        "newRole": req.requested_role if body.status == "approved" else req.current_role,
    }


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

    import os as _os2  # noqa: F811
    token = session_store.create(user.username, user.role)
    origin = _os2.getenv("APP_FRONTEND_ORIGIN", "http://127.0.0.1:5173")
    frontend_url = f"{origin}{redirect}?token={token}&username={user.username}&role={user.role}"
    return RedirectResponse(url=frontend_url)


def _build_feishu_url(state: str, redirect: str) -> str:
    from app.services.feishu_service import build_auth_url

    callback = (
        FEISHU_REDIRECT_URI
        + "?" + urlencode({"state": state, "redirect": redirect})
    )
    return build_auth_url(state=state, redirect_uri=callback)


# ── Google OAuth ─────────────────────────────────────────────────

import json as _json


@router.get("/google/auth-url")
def google_auth_url(
    redirect: str = Query("/", description="Frontend page to return to"),
) -> dict:
    """Return the Google OAuth authorization URL."""
    if not GOOGLE_ENABLED:
        raise HTTPException(status_code=503, detail="Google login not configured")
    # Encode redirect destination into the state param (Google passes it back)
    state = _json.dumps({
        "redirect": redirect,
        "nonce": secrets.token_urlsafe(8),
    })
    from app.services.google_service import build_auth_url

    url = build_auth_url(state=state, redirect_uri=GOOGLE_REDIRECT_URI)
    return {"url": url}


@router.get("/google/callback")
def google_callback(
    code: str = Query(...),
    state: str = Query(...),
    db: Session = Depends(get_db_session),
) -> RedirectResponse:
    """Google OAuth callback — exchange code, find/create user, redirect."""
    if not GOOGLE_ENABLED:
        raise HTTPException(status_code=503, detail="Google login not configured")

    # Decode redirect destination from state
    redirect = "/"
    try:
        state_data = _json.loads(state)
        redirect = state_data.get("redirect", "/")
    except (_json.JSONDecodeError, TypeError):
        pass

    from app.services.google_service import exchange_code

    try:
        user_info = exchange_code(code, GOOGLE_REDIRECT_URI)
    except Exception as exc:
        raise HTTPException(
            status_code=401, detail=f"Google auth failed: {exc}"
        ) from exc

    email = str(user_info.get("email") or "").strip()
    google_id = str(user_info.get("sub") or "")

    if not email or not google_id:
        raise HTTPException(status_code=401, detail="Missing Google account info")

    # Use email prefix as username
    username = email.split("@")[0]

    user = db.query(User).filter(User.username == username).first()
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

    import os as _os
    token = session_store.create(user.username, user.role)
    origin = _os.getenv("APP_FRONTEND_ORIGIN", "http://127.0.0.1:5173")
    frontend_url = f"{origin}{redirect}?token={token}&username={user.username}&role={user.role}"
    return RedirectResponse(url=frontend_url)
