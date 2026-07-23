import pytest
from fastapi import HTTPException

from app.core import security
from app.core.security import UserContext


def test_protected_current_user_rejects_anonymous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)

    with pytest.raises(HTTPException) as exc_info:
        security.get_current_user(x_auth_token=None, x_user_name="")

    assert exc_info.value.status_code == 401


def test_protected_current_user_rejects_invalid_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {})

    with pytest.raises(HTTPException) as exc_info:
        security.get_current_user(
            x_auth_token="expired-or-invalid",
            x_user_name="stale-user",
        )

    assert exc_info.value.status_code == 401


def test_static_token_role_map_still_applies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"editor-token": "editor"})
    monkeypatch.setattr(security, "TOKEN_ACTOR_MAP", {})

    user = security.get_current_user(
        x_auth_token="editor-token",
        x_user_name="ops",
    )

    assert user == UserContext(role="editor", name="static-editor")


def test_static_token_uses_server_owned_actor_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"editor-token": "editor"})
    monkeypatch.setattr(
        security,
        "TOKEN_ACTOR_MAP",
        {"editor-token": "config-importer"},
    )

    user = security.get_current_user(
        x_auth_token="editor-token",
        x_user_name="forged-user",
    )

    assert user == UserContext(role="editor", name="config-importer")


def test_editor_endpoint_rejects_anonymous_viewer() -> None:
    dependency = security.require_min_role("editor")

    with pytest.raises(HTTPException) as exc_info:
        dependency(UserContext(role="viewer", name="anonymous"))

    assert exc_info.value.status_code == 403


def test_admin_includes_viewer_permissions() -> None:
    dependency = security.require_min_role("viewer")

    user = dependency(UserContext(role="admin", name="admin"))

    assert user == UserContext(role="admin", name="admin")
