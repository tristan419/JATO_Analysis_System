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
    is_admin,
    ORDERING_BRANDS,
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
    brands: list[str] = Field(default_factory=list)
    preferredLandingPage: str | None = None
    profileComplete: bool = False


class UserProfileBody(BaseModel):
    brands: list[str] | None = None
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
        "brands": list(user.brands or []),
        "preferredLandingPage": user.preferred_landing_page,
        "profileComplete": bool(user.primary_country_code),
    }


def _copy_user_payload(payload: dict) -> dict:
    """Return a defensive copy so cached list values cannot be mutated by callers."""
    copied = dict(payload)
    copied["secondaryCountries"] = list(payload.get("secondaryCountries") or [])
    copied["brands"] = list(payload.get("brands") or [])
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
        brands=user.brands or [],
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

    if body.brands is not None:
        if not is_admin(user.role):
            raise HTTPException(403, "Brands are managed by admin / 品牌由管理员分配")
        db_user.brands = _normalize_brands(body.brands)

    _apply_profile_fields(db_user, body, allow_countries=user.role != "order_filler")
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
    if body.brands is not None:
        user.brands = _normalize_brands(body.brands)
    _apply_profile_fields(user, body, allow_countries=True)
    db.commit()
    db.refresh(user)
    _invalidate_me_payload_cache(user.username)
    return _user_payload(user)


def _normalize_brands(values: list[str]) -> list[str]:
    brands = {str(value).strip().upper() for value in values}
    if not brands <= set(ORDERING_BRANDS):
        raise HTTPException(400, "Unknown brand / 未知品牌")
    return [brand for brand in ORDERING_BRANDS if brand in brands]


def _apply_profile_fields(user: User, body: UserProfileBody, *, allow_countries: bool) -> None:
    fields = body.model_fields_set
    primary = (_normalize_country_code(body.primary_country)
               if "primary_country" in fields else user.primary_country_code)
    secondary = _normalize_secondary_countries(
        body.secondary_countries if "secondary_countries" in fields else user.secondary_country_codes,
        primary,
    )
    if not allow_countries and (primary != user.primary_country_code
                               or secondary != (user.secondary_country_codes or [])):
        raise HTTPException(403, "Country assignments are managed by your administrator")
    if {"primary_country", "secondary_countries"} & fields:
        user.primary_country_code = primary
        user.secondary_country_codes = secondary
    if "preferred_landing_page" in fields:
        user.preferred_landing_page = str(body.preferred_landing_page or "").strip() or None
    if "display_name" in fields:
        user.display_name = str(body.display_name or "").strip() or None


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
    requested_role: str = "editor"
    requested_brands: list[str] | None = Field(default=None, alias="requestedBrands")
    reason: str = ""
    model_config = {"populate_by_name": True}


class ReviewUpgradeBody(BaseModel):
    status: str  # "approved" or "rejected"
    brands: list[str] | None = None


@router.post("/role-upgrade/request")
def request_role_upgrade(
    body: RoleUpgradeRequestBody,
    db: Session = Depends(get_db_session),
    user: UserContext = Depends(get_current_user),
) -> dict:
    brands = _normalize_brands(body.requested_brands) if body.requested_brands is not None else None
    if brands is not None and (not brands or not body.reason.strip() or user.role not in {"order_filler", "editor"}):
        raise HTTPException(400, "Select brands and provide a reason / 请选择品牌并填写申请说明")
    if brands is None and body.requested_role != "editor":
        raise HTTPException(
            status_code=400,
            detail="Users can only request editor access; admin is assigned manually.",
        )
    if brands is None and ROLE_LEVEL.get(body.requested_role, 0) <= ROLE_LEVEL.get(user.role, 0):
        raise HTTPException(status_code=400, detail="Cannot downgrade or request same level")

    existing = (
        db.query(RoleUpgradeRequest)
        .filter(
            RoleUpgradeRequest.username == user.name,
            RoleUpgradeRequest.status == "pending",
            (RoleUpgradeRequest.requested_brands.is_not(None) if brands is not None
             else RoleUpgradeRequest.requested_brands.is_(None)),
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
        requested_role=user.role if brands is not None else body.requested_role,
        requested_brands=brands,
        reason=body.reason,
        status="pending",
    )
    db.add(req)
    db.commit()
    return {
        "requestId": str(req.request_id),
        "username": req.username,
        "requestedRole": req.requested_role,
        "requestedBrands": req.requested_brands,
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
                "requestedBrands": r.requested_brands,
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

    db_user = db.query(User).filter(User.username == req.username).first()
    if body.status == "approved":
        if req.requested_brands is None and req.requested_role != "editor":
            raise HTTPException(
                status_code=400,
                detail="Only editor requests can be approved through this flow",
            )
        if not db_user:
            raise HTTPException(404, "Stored user not found")
        if req.requested_brands is not None:
            brands = _normalize_brands(body.brands if body.brands is not None else req.requested_brands)
            if not brands:
                raise HTTPException(400, "Approve at least one brand / 至少批准一个品牌")
            db_user.brands = _normalize_brands([*(db_user.brands or []), *brands])
            _invalidate_me_payload_cache(db_user.username)
        else:
            db_user.role = req.requested_role
            _invalidate_me_payload_cache(db_user.username)

    req.status = body.status
    req.reviewed_by = admin.name
    req.reviewed_at_utc = datetime.now(timezone.utc)
    db.commit()
    return {
        "requestId": str(req.request_id),
        "status": req.status,
        "username": req.username,
        "newRole": db_user.role if db_user else req.current_role,
        "brands": list(db_user.brands or []) if db_user else [],
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
    frontend_origin: str | None = None,
) -> dict:
    """Return the Google OAuth authorization URL."""
    if not GOOGLE_ENABLED:
        raise HTTPException(status_code=503, detail="Google login not configured")
    return_origin = _allowed_frontend_origin(frontend_origin) or _frontend_origin_for_request(request)
    # Encode redirect destination into the state param (Google passes it back)
    state = _json.dumps({
        "redirect": _safe_frontend_redirect(redirect),
        "frontend_origin": return_origin,
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
