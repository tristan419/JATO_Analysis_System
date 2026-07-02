from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.routes import auth
from app.core.security import UserContext
from app.db.models import User


class _FakeQuery:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def filter(self, *_args) -> "_FakeQuery":
        return self

    def first(self) -> User | None:
        self._db.first_calls += 1
        return self._db.user


class _FakeDb:
    def __init__(self, user: User | None) -> None:
        self.user = user
        self.first_calls = 0
        self.commits = 0

    def query(self, _model) -> _FakeQuery:
        return _FakeQuery(self)

    def commit(self) -> None:
        self.commits += 1

    def refresh(self, _user: User) -> None:
        return None


def test_normalize_secondary_countries_deduplicates_and_excludes_primary() -> None:
    assert auth._normalize_secondary_countries(
        [" se ", "cz", "SE", "", "pl"],
        "SE",
    ) == ["CZ", "PL"]


def test_normalize_country_code_rejects_invalid_long_value() -> None:
    with pytest.raises(HTTPException):
        auth._normalize_country_code("SWEDEN-LONG")


def test_user_payload_includes_country_profile_fields() -> None:
    user = User(
        id=uuid4(),
        username="tristan",
        password_hash="hash",
        role="editor",
        is_active=True,
        primary_country_code="SE",
        secondary_country_codes=["CZ", "PL"],
        preferred_landing_page="/product/order-genius",
    )

    payload = auth._user_payload(user)

    assert payload["username"] == "tristan"
    assert payload["role"] == "editor"
    assert payload["primaryCountry"] == "SE"
    assert payload["secondaryCountries"] == ["CZ", "PL"]
    assert payload["preferredLandingPage"] == "/product/order-genius"
    assert payload["profileComplete"] is True


def test_me_reuses_short_ttl_user_payload_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth._clear_me_payload_cache()
    monkeypatch.setattr(auth, "AUTH_ENABLED", True)
    user = User(
        id=uuid4(),
        username="tristan",
        password_hash="hash",
        role="editor",
        is_active=True,
        primary_country_code="SE",
        secondary_country_codes=["CZ"],
        preferred_landing_page="/dashboard",
    )
    db = _FakeDb(user)
    context = UserContext(role="editor", name="tristan")

    first_payload = auth.me(db=db, user=context)
    second_payload = auth.me(db=db, user=context)
    second_payload["secondaryCountries"].append("MUTATED")
    third_payload = auth.me(db=db, user=context)

    assert db.first_calls == 1
    assert first_payload["username"] == "tristan"
    assert second_payload["secondaryCountries"] == ["CZ", "MUTATED"]
    assert third_payload["secondaryCountries"] == ["CZ"]
    auth._clear_me_payload_cache()


def test_update_user_role_invalidates_cached_me_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth._clear_me_payload_cache()
    monkeypatch.setattr(auth, "AUTH_ENABLED", True)
    user = User(
        id=uuid4(),
        username="tristan",
        password_hash="hash",
        role="viewer",
        is_active=True,
    )
    auth._store_me_payload(
        user.username,
        {
            "username": user.username,
            "role": user.role,
            "secondaryCountries": [],
        },
    )
    db = _FakeDb(user)

    result = auth.update_user_role(
        str(user.id),
        auth.UpdateRoleBody(role="editor"),
        db=db,
        _=UserContext(role="admin", name="admin"),
    )

    assert result["role"] == "editor"
    assert user.role == "editor"
    assert db.commits == 1
    assert auth._get_cached_me_payload(user.username) is None
    auth._clear_me_payload_cache()
