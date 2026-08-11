from collections.abc import Callable

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import main as app_main
from app.core import security
from app.core.security import UserContext
from app.services.auth_service import SessionToken


def test_anonymous_current_user_is_viewer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security, "AUTH_REQUIRED", False)

    user = security.get_current_user(x_auth_token=None, x_user_name="")

    assert user == UserContext(role="viewer", name="anonymous")


def test_invalid_token_downgrades_to_anonymous_viewer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security, "AUTH_REQUIRED", False)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {})

    user = security.get_current_user(
        x_auth_token="expired-or-invalid",
        x_user_name="stale-user",
    )

    assert user == UserContext(role="viewer", name="stale-user")


def test_static_token_role_map_still_applies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security, "AUTH_REQUIRED", False)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"editor-token": "editor"})

    user = security.get_current_user(
        x_auth_token="editor-token",
        x_user_name="ops",
    )

    assert user == UserContext(role="editor", name="ops")


@pytest.mark.parametrize(
    "dependency",
    [security.get_current_user, security.get_optional_user],
)
def test_required_auth_rejects_missing_or_invalid_token(
    monkeypatch: pytest.MonkeyPatch,
    dependency: Callable[..., UserContext],
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security, "AUTH_REQUIRED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {})

    with pytest.raises(HTTPException) as missing:
        dependency(x_auth_token=None, x_user_name="admin")
    with pytest.raises(HTTPException) as invalid:
        dependency(x_auth_token="invalid", x_user_name="admin")

    assert missing.value.status_code == 401
    assert invalid.value.status_code == 401


def test_required_auth_uses_verified_token_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security, "AUTH_REQUIRED", True)
    monkeypatch.setattr(
        security.session_store,
        "lookup",
        lambda _token: SessionToken(username="sandbox-admin", role="admin"),
    )

    user = security.get_current_user(
        x_auth_token="candidate-jwt",
        x_user_name="spoofed-user",
    )

    assert user == UserContext(role="admin", name="sandbox-admin")


def test_required_auth_middleware_guards_all_non_public_backend_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(app_main, "AUTH_REQUIRED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {})
    client = TestClient(app_main.app)

    protected = client.get("/openapi.json", headers={"X-User-Name": "admin"})
    spoofed_me = client.get(
        "/v1/auth/me",
        headers={
            "X-Auth-Token": "invalid",
            "X-User-Name": "admin",
        },
    )
    health = client.get("/healthz")
    login = client.post("/v1/auth/login", json={})
    monkeypatch.setattr(
        security.session_store,
        "lookup",
        lambda _token: SessionToken(username="sandbox-admin", role="admin"),
    )
    authenticated = client.get(
        "/openapi.json",
        headers={"X-Auth-Token": "candidate-jwt"},
    )

    assert protected.status_code == 401
    assert protected.json() == {"detail": "Authentication required"}
    assert spoofed_me.status_code == 401
    assert health.status_code == 200
    assert login.status_code != 401
    assert authenticated.status_code == 200


def test_editor_endpoint_rejects_anonymous_viewer() -> None:
    dependency = security.require_min_role("editor")

    with pytest.raises(HTTPException) as exc_info:
        dependency(UserContext(role="viewer", name="anonymous"))

    assert exc_info.value.status_code == 403


def test_admin_includes_viewer_permissions() -> None:
    dependency = security.require_min_role("viewer")

    user = dependency(UserContext(role="admin", name="admin"))

    assert user == UserContext(role="admin", name="admin")
