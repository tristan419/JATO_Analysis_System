"""Auth routes — login, register, session."""

from __future__ import annotations

import logging
import secrets
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urlparse

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.config import (
    APP_FRONTEND_ORIGIN,
    AUTH_ENABLED,
    FEISHU_ENABLED,
    FEISHU_REDIRECT_URI,
    FRONTEND_ORIGINS,
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
log = logging.getLogger(__name__)

_ME_PAYLOAD_CACHE_TTL_SECONDS = 15
_ME_PAYLOAD_CACHE_MAX_ENTRIES = 512
_me_payload_cache: dict[str, tuple[float, dict]] = {}
_me_payload_cache_lock = threading.Lock()


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
    email: str | None = None
    oauthProvider: str | None = None
    avatarUrl: str | None = None
    displayName: str | None = None
    primaryCountry: str | None = None
    secondaryCountries: list[str] = Field(default_factory=list)
    preferredLandingPage: str | None = None
    profileComplete: bool = False


class UserProfileBody(BaseModel):
    primary_country: str | None = Field(default=None, alias="primaryCountry")
    secondary_countries: list[str] = Field(
        default_factory=list,
        alias="secondaryCountries",
    )
    preferred_landing_page: str | None = Field(
        default=None,
        alias="preferredLandingPage",
    )
    display_name: str | None = Field(default=None, alias="displayName")

    model_config = {"populate_by_name": True}


def _normalize_country_code(value: str | None) -> str | None:
    code = str(value or "").strip().upper()
    if not code:
        return None
    if len(code) > 8:
        raise HTTPException(status_code=400, detail="Invalid country code")
    return code


def _normalize_secondary_countries(
    values: list[str] | None,
    primary: str | None,
) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values or []:
        code = _normalize_country_code(raw)
        if not code or code == primary or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def _user_payload(user: User) -> dict:
    secondary = list(user.secondary_country_codes or [])
    return {
        "id": str(user.id),
        "username": user.username,
        "role": user.role,
        "isActive": user.is_active,
        "email": user.email,
        "oauthProvider": user.oauth_provider,
        "avatarUrl": user.avatar_url,
        "displayName": user.display_name,
        "primaryCountry": user.primary_country_code,
        "secondaryCountries": secondary,
        "preferredLandingPage": user.preferred_landing_page,
        "profileComplete": bool(user.primary_country_code),
    }


def _copy_user_payload(payload: dict) -> dict:
    """Return a defensive copy so cached list values cannot be mutated by callers."""
    copied = dict(payload)
    copied["secondaryCountries"] = list(payload.get("secondaryCountries") or [])
    return copied


def _get_cached_me_payload(username: str) -> dict | None:
    if not AUTH_ENABLED:
        return None
    key = str(username or "").strip()
    if not key or key == "anonymous":
        return None

    now = time.monotonic()
    with _me_payload_cache_lock:
        cached = _me_payload_cache.get(key)
        if cached is None:
            return None
        cached_at, payload = cached
        if (now - cached_at) >= _ME_PAYLOAD_CACHE_TTL_SECONDS:
            _me_payload_cache.pop(key, None)
            return None
        return _copy_user_payload(payload)


def _store_me_payload(username: str, payload: dict) -> dict:
    if not AUTH_ENABLED:
        return payload
    key = str(username or "").strip()
    if not key or key == "anonymous":
        return payload

    with _me_payload_cache_lock:
        _me_payload_cache[key] = (time.monotonic(), _copy_user_payload(payload))
        while len(_me_payload_cache) > _ME_PAYLOAD_CACHE_MAX_ENTRIES:
            oldest_key = min(
                _me_payload_cache,
                key=lambda item: _me_payload_cache[item][0],
            )
            _me_payload_cache.pop(oldest_key, None)
    return payload


def _invalidate_me_payload_cache(username: str | None) -> None:
    key = str(username or "").strip()
    if not key:
        return
    with _me_payload_cache_lock:
        _me_payload_cache.pop(key, None)


def _clear_me_payload_cache() -> None:
    with _me_payload_cache_lock:
        _me_payload_cache.clear()


def _safe_frontend_redirect(value: str | None) -> str:
    redirect = str(value or "/").strip() or "/"
    if not redirect.startswith("/") or redirect.startswith("//"):
        return "/"
    return redirect


def _frontend_origin() -> str:
    return APP_FRONTEND_ORIGIN.rstrip("/")


def _origin_from_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _allowed_frontend_origin(value: str | None) -> str | None:
    candidate = _origin_from_url(value) or str(value or "").strip().rstrip("/")
    if candidate and candidate in {origin.rstrip("/") for origin in FRONTEND_ORIGINS}:
        return candidate
    return None


def _frontend_origin_for_request(request: Request) -> str:
    origin = _allowed_frontend_origin(request.headers.get("origin"))
    if origin:
        return origin
    referer_origin = _origin_from_url(request.headers.get("referer"))
    origin = _allowed_frontend_origin(referer_origin)
    return origin or _frontend_origin()


def _frontend_url(
    path: str,
    params: dict[str, str],
    origin: str | None = None,
) -> str:
    safe_path = _safe_frontend_redirect(path)
    frontend_origin = _allowed_frontend_origin(origin) or _frontend_origin()
    if not params:
        return f"{frontend_origin}{safe_path}"
    separator = "&" if "?" in safe_path else "?"
    return f"{frontend_origin}{safe_path}{separator}{urlencode(params)}"


def _oauth_error_redirect(
    message: str,
    redirect: str,
    origin: str | None = None,
) -> RedirectResponse:
    return RedirectResponse(
        url=_frontend_url(
            "/login",
            {
                "oauthError": message,
                "redirect": _safe_frontend_redirect(redirect),
            },
            origin,
        )
    )


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
        email=user.email,
        oauthProvider=user.oauth_provider,
        avatarUrl=user.avatar_url,
        displayName=user.display_name,
        primaryCountry=user.primary_country_code,
        secondaryCountries=user.secondary_country_codes or [],
        preferredLandingPage=user.preferred_landing_page,
        profileComplete=bool(user.primary_country_code),
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
    if body.role not in ("admin", "editor", "viewer", "order_filler"):
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
def me(
    db: Session = Depends(get_db_session),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Return the current authenticated user."""
    cached_payload = _get_cached_me_payload(user.name)
    if cached_payload is not None:
        return cached_payload

    db_user = db.query(User).filter(User.username == user.name).first()
    if not db_user:
        return {
            "username": user.name,
            "role": user.role,
            "primaryCountry": None,
            "secondaryCountries": [],
            "preferredLandingPage": None,
            "profileComplete": False,
        }
    payload = _user_payload(db_user)
    # When auth is disabled, the context role (admin) takes precedence over the DB role
    # so that local development always sees full admin permissions.
    if not AUTH_ENABLED:
        payload["role"] = user.role
        return payload
    return _store_me_payload(user.name, payload)


@router.patch("/me/profile")
def update_my_profile(
    body: UserProfileBody,
    db: Session = Depends(get_db_session),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Update the current user's country preferences."""
    db_user = db.query(User).filter(User.username == user.name).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    # order_filler users cannot modify their own country assignments — only admin can
    if db_user.role == "order_filler":
        new_primary = _normalize_country_code(body.primary_country)
        new_secondary = _normalize_secondary_countries(body.secondary_countries, new_primary)
        current_secondary = db_user.secondary_country_codes or []
        if (new_primary != db_user.primary_country_code or
                new_secondary != current_secondary):
            raise HTTPException(status_code=403, detail="Country assignments are managed by your administrator")
        # Allow display name and preferred landing page changes
        db_user.preferred_landing_page = (
            str(body.preferred_landing_page).strip()
            if body.preferred_landing_page
            else None
        )
        if body.display_name is not None:
            dn = str(body.display_name).strip()
            db_user.display_name = dn if dn else None
        db.commit()
        db.refresh(db_user)
        _invalidate_me_payload_cache(db_user.username)
        return _user_payload(db_user)

    primary = _normalize_country_code(body.primary_country)
    db_user.primary_country_code = primary
    db_user.secondary_country_codes = _normalize_secondary_countries(
        body.secondary_countries,
        primary,
    )
    db_user.preferred_landing_page = (
        str(body.preferred_landing_page).strip()
        if body.preferred_landing_page
        else None
    )
    if body.display_name is not None:
        dn = str(body.display_name).strip()
        db_user.display_name = dn if dn else None
    db.commit()
    db.refresh(db_user)
    _invalidate_me_payload_cache(db_user.username)
    return _user_payload(db_user)


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
    if body.role not in ("admin", "editor", "viewer", "order_filler"):
        raise HTTPException(status_code=400, detail="Invalid role")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = body.role
    db.commit()
    _invalidate_me_payload_cache(user.username)
    return {"id": str(user.id), "username": user.username, "role": user.role}


@router.patch("/users/{user_id}/profile")
def update_user_profile(
    user_id: str,
    body: UserProfileBody,
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Update a user's country preferences. Admin only."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    primary = _normalize_country_code(body.primary_country)
    user.primary_country_code = primary
    user.secondary_country_codes = _normalize_secondary_countries(
        body.secondary_countries,
        primary,
    )
    user.preferred_landing_page = (
        str(body.preferred_landing_page).strip()
        if body.preferred_landing_page
        else None
    )
    if body.display_name is not None:
        dn = str(body.display_name).strip()
        user.display_name = dn if dn else None
    db.commit()
    db.refresh(user)
    _invalidate_me_payload_cache(user.username)
    return _user_payload(user)


@router.delete("/users/{user_id}")
def delete_user(
    user_id: str,
    db: Session = Depends(get_db_session),
    admin: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Hard-delete a user. Admin only. Cannot delete yourself."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.username == admin.name:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    username = user.username
    db.delete(user)
    db.commit()
    _invalidate_me_payload_cache(username)
    return {"id": str(user_id), "username": username, "deleted": True}


@router.patch("/users/{user_id}/toggle-active")
def toggle_user_active(
    user_id: str,
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Toggle a user's is_active flag. Admin only."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = not user.is_active
    db.commit()
    db.refresh(user)
    _invalidate_me_payload_cache(user.username)
    return _user_payload(user)


class ResetPasswordBody(BaseModel):
    password: str = Field(min_length=6)


@router.patch("/users/{user_id}/password")
def reset_user_password(
    user_id: str,
    body: ResetPasswordBody,
    db: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    """Reset a user's password. Admin only."""
    from app.services.auth_service import hash_password

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.password_hash = hash_password(body.password)
    db.commit()
    _invalidate_me_payload_cache(user.username)
    return {"id": str(user_id), "username": user.username, "passwordReset": True}


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
    if body.requested_role != "editor":
        raise HTTPException(
            status_code=400,
            detail="Users can only request editor access; admin is assigned manually.",
        )
    if ROLE_LEVEL.get(body.requested_role, 0) <= ROLE_LEVEL.get(user.role, 0):
        raise HTTPException(status_code=400, detail="Cannot downgrade or request same level")

    existing = (
        db.query(RoleUpgradeRequest)
        .filter(
            RoleUpgradeRequest.username == user.name,
            RoleUpgradeRequest.status == "pending",
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="You already have a pending request")

    db_user = db.query(User).filter(User.username == user.name).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="Stored user not found")
    req = RoleUpgradeRequest(
        user_id=db_user.id,
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
        if req.requested_role != "editor":
            raise HTTPException(
                status_code=400,
                detail="Only editor requests can be approved through this flow",
            )
        db_user = db.query(User).filter(User.username == req.username).first()
        if db_user:
            db_user.role = req.requested_role
            _invalidate_me_payload_cache(db_user.username)

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
    request: Request,
    redirect: str = Query("/", description="Frontend page to return to"),
) -> dict:
    """Return the Feishu authorization URL."""
    if not FEISHU_ENABLED:
        raise HTTPException(status_code=503, detail="Feishu login not configured")
    state = secrets.token_urlsafe(16)
    url = _build_feishu_url(state, redirect, _frontend_origin_for_request(request))
    return {"url": url, "state": state}


@router.get("/feishu/callback")
def feishu_callback(
    code: str = Query(...),
    state: str = Query(...),
    redirect: str = Query("/"),
    frontend_origin: str | None = Query(None),
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
    frontend_url = _frontend_url(
        redirect,
        {
            "token": token,
            "username": user.username,
            "role": user.role,
        },
        frontend_origin,
    )
    return RedirectResponse(url=frontend_url)


def _build_feishu_url(
    state: str,
    redirect: str,
    frontend_origin: str,
) -> str:
    from app.services.feishu_service import build_auth_url

    callback = (
        FEISHU_REDIRECT_URI
        + "?"
        + urlencode(
            {
                "state": state,
                "redirect": _safe_frontend_redirect(redirect),
                "frontend_origin": frontend_origin,
            }
        )
    )
    return build_auth_url(state=state, redirect_uri=callback)


# ── Google OAuth ─────────────────────────────────────────────────

import json as _json


@router.get("/google/auth-url")
def google_auth_url(
    request: Request,
    redirect: str = Query("/", description="Frontend page to return to"),
) -> dict:
    """Return the Google OAuth authorization URL."""
    if not GOOGLE_ENABLED:
        raise HTTPException(status_code=503, detail="Google login not configured")
    # Encode redirect destination into the state param (Google passes it back)
    state = _json.dumps({
        "redirect": _safe_frontend_redirect(redirect),
        "frontend_origin": _frontend_origin_for_request(request),
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
    frontend_origin = None
    try:
        state_data = _json.loads(state)
        if isinstance(state_data, dict):
            redirect = _safe_frontend_redirect(state_data.get("redirect", "/"))
            frontend_origin = _allowed_frontend_origin(
                str(state_data.get("frontend_origin") or "")
            )
    except (_json.JSONDecodeError, TypeError):
        pass

    from app.services.google_service import GoogleOAuthError, exchange_code

    try:
        user_info = exchange_code(code, GOOGLE_REDIRECT_URI)
    except GoogleOAuthError as exc:
        return _oauth_error_redirect(str(exc), redirect, frontend_origin)
    except Exception:
        log.exception("Unexpected Google OAuth callback failure")
        return _oauth_error_redirect(
            "Google auth failed: unexpected server error.",
            redirect,
            frontend_origin,
        )

    email = str(user_info.get("email") or "").strip()
    google_id = str(user_info.get("id") or user_info.get("sub") or "")
    name = str(user_info.get("name") or "").strip()
    picture = str(user_info.get("picture") or "").strip()

    if not email or not google_id:
        return _oauth_error_redirect(
            "Missing Google account info",
            redirect,
            frontend_origin,
        )

    # 1. Find by OAuth subject (returning Google user)
    user = (
        db.query(User)
        .filter(
            User.oauth_provider == "google",
            User.oauth_subject == google_id,
        )
        .first()
    )
    is_new = False

    if user:
        # Sync avatar and email from Google (preserve user-set display_name)
        dirty = False
        if picture and user.avatar_url != picture:
            user.avatar_url = picture
            dirty = True
        if email and user.email != email:
            user.email = email
            dirty = True
        # Keep display_name if user has customized it; only set if null
        if name and not user.display_name:
            user.display_name = name
            dirty = True
        if dirty:
            db.commit()
            db.refresh(user)
    else:
        # 2. Find by email (existing password user linking Google for first time)
        user = db.query(User).filter(User.email == email).first()
        if user:
            # Link Google account to existing user
            user.oauth_provider = "google"
            user.oauth_subject = google_id
            if picture and not user.avatar_url:
                user.avatar_url = picture
            if email:
                user.email = email
            if name and not user.display_name:
                user.display_name = name
            db.commit()
            db.refresh(user)
        else:
            # 3. Create new user (first-time Google registration)
            from uuid import uuid4
            from app.services.auth_service import hash_password

            base_username = email.split("@")[0]
            username = base_username
            # Ensure unique username
            existing = (
                db.query(User).filter(User.username == username).first()
            )
            suffix = 1
            while existing:
                username = f"{base_username}{suffix}"
                existing = (
                    db.query(User)
                    .filter(User.username == username)
                    .first()
                )
                suffix += 1

            user = User(
                id=uuid4(),
                username=username,
                email=email,
                display_name=name or None,
                password_hash=hash_password(secrets.token_urlsafe(16)),
                role="viewer",
                is_active=True,
                oauth_provider="google",
                oauth_subject=google_id,
                avatar_url=picture or None,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            is_new = True

    token = session_store.create(user.username, user.role)
    params = {
        "token": token,
        "username": user.username,
        "role": user.role,
    }
    if is_new:
        params["isNewUser"] = "true"
    frontend_url = _frontend_url(redirect, params, frontend_origin)
    return RedirectResponse(url=frontend_url)
