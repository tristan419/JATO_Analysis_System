"""Auth service — password hashing, login sessions, user CRUD."""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from dataclasses import dataclass, field
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


# ── In-memory session token store ─────────────────────────────────


@dataclass
class SessionToken:
    username: str
    role: str
    created_at: float = field(default_factory=time.time)


class SessionStore:
    """In-memory token → user mapping. Survives requests, dies on restart."""

    def __init__(self, ttl_hours: int = 24) -> None:
        self._tokens: dict[str, SessionToken] = {}
        self._ttl = ttl_hours * 3600
        self._lock = threading.Lock()

    def create(self, username: str, role: str) -> str:
        """Generate a new session token and return it."""
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._cleanup()
            self._tokens[token] = SessionToken(username=username, role=role)
        return token

    def lookup(self, token: str) -> SessionToken | None:
        """Resolve a token to a session, or None if expired/unknown."""
        with self._lock:
            self._cleanup()
            return self._tokens.get(token)

    def revoke(self, token: str) -> None:
        with self._lock:
            self._tokens.pop(token, None)

    def _cleanup(self) -> None:
        now = time.time()
        expired = [
            tok
            for tok, sess in self._tokens.items()
            if now - sess.created_at > self._ttl
        ]
        for tok in expired:
            del self._tokens[tok]


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
            "created_at_utc": u.created_at_utc.isoformat() if u.created_at_utc else None,
        }
        for u in users
    ]
