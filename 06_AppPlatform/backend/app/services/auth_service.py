"""Auth service — password hashing, JWT sessions, user CRUD."""

from __future__ import annotations

import hashlib
import hmac
import json as _json
import os
import secrets
import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db.models import User

# ── Password hashing ─────────────────────────────────────────────

_SALT_BYTES = 32
_HASH_ITERATIONS = 600_000
_HASH_BYTES = 32


def hash_password(password: str) -> str:
    """PBKDF2-SHA256 with random salt. Returns '$pbkdf2$<salt_hex>$<hash_hex>'."""
    salt = secrets.token_bytes(_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), salt, _HASH_ITERATIONS, dklen=_HASH_BYTES
    )
    return f"$pbkdf2${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Verify a password against a stored hash."""
    try:
        _, _, salt_hex, hash_hex = stored.split("$")
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        dk = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode(),
            salt,
            _HASH_ITERATIONS,
            dklen=len(expected),
        )
        return secrets.compare_digest(dk, expected)
    except (ValueError, IndexError):
        return False


# ── JWT session tokens (multi-worker safe, survives restarts) ─────

import base64

_JWT_SECRET = os.getenv("APP_JWT_SECRET", "change-me-jwt-secret").encode()
_JWT_TTL = 24 * 3600  # 24 hours


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _b64url_decode(data: str) -> bytes:
    padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
    return base64.urlsafe_b64decode(padded)


def jwt_encode(payload: dict, ttl: int = _JWT_TTL) -> str:
    """HS256 JWT: header.payload.signature."""
    header = _b64url_encode(_json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url_encode(_json.dumps({
        **payload, "exp": int(time.time()) + ttl, "iat": int(time.time()),
    }).encode())
    sig = hmac.new(_JWT_SECRET, f"{header}.{body}".encode(), "sha256").digest()
    return f"{header}.{body}.{_b64url_encode(sig)}"


def jwt_decode(token: str) -> dict | None:
    """Verify and decode a JWT. Returns payload dict or None."""
    try:
        header, body, sig = token.split(".")
        expected = hmac.new(_JWT_SECRET, f"{header}.{body}".encode(), "sha256").digest()
        if not secrets.compare_digest(_b64url_decode(sig), expected):
            return None
        payload = _json.loads(_b64url_decode(body))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


@dataclass
class SessionToken:
    username: str
    role: str


class SessionStore:
    """Stateless JWT session store — no in-memory state, multi-worker safe."""

    def create(self, username: str, role: str) -> str:
        return jwt_encode({"username": username, "role": role})

    def lookup(self, token: str) -> SessionToken | None:
        payload = jwt_decode(token)
        if not payload:
            return None
        return SessionToken(
            username=payload.get("username", ""),
            role=payload.get("role", "viewer"),
        )

    def revoke(self, token: str) -> None:
        pass  # JWT is self-expiring; add a blocklist here if needed


session_store = SessionStore()


# ── User CRUD ─────────────────────────────────────────────────────


def create_user(
    db: Session,
    username: str,
    password: str,
    role: str = "viewer",
) -> User:
    """Create a new user. Raises ValueError if username exists."""
    existing = db.query(User).filter(User.username == username).first()
    if existing:
        raise ValueError(f"User '{username}' already exists")
    user = User(
        id=uuid4(),
        username=username,
        password_hash=hash_password(password),
        role=role,
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate(db: Session, username: str, password: str) -> str | None:
    """Validate credentials. Returns a session token on success, None on failure."""
    user = db.query(User).filter(User.username == username).first()
    if not user or not user.is_active:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return session_store.create(user.username, user.role)


def list_users(db: Session) -> list[dict]:
    """List all users (admin use)."""
    users = db.query(User).order_by(User.username).all()
    return [
        {
            "id": str(u.id),
            "username": u.username,
            "role": u.role,
            "is_active": u.is_active,
            "primary_country_code": u.primary_country_code,
            "secondary_country_codes": u.secondary_country_codes or [],
            "preferred_landing_page": u.preferred_landing_page,
            "created_at_utc": u.created_at_utc.isoformat() if u.created_at_utc else None,
        }
        for u in users
    ]
