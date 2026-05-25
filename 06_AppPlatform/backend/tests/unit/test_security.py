import pytest
from fastapi import HTTPException

from app.core import security
from app.core.security import UserContext


def test_anonymous_current_user_is_viewer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)

    user = security.get_current_user(x_auth_token=None, x_user_name="")

    assert user == UserContext(role="viewer", name="anonymous")


def test_invalid_token_downgrades_to_anonymous_viewer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(security, "AUTH_ENABLED", True)
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
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"editor-token": "editor"})

    user = security.get_current_user(
        x_auth_token="editor-token",
        x_user_name="ops",
    )

    assert user == UserContext(role="editor", name="ops")


def test_editor_endpoint_rejects_anonymous_viewer() -> None:
    dependency = security.require_min_role("editor")

    with pytest.raises(HTTPException) as exc_info:
        dependency(UserContext(role="viewer", name="anonymous"))

    assert exc_info.value.status_code == 403


def test_admin_includes_viewer_permissions() -> None:
    dependency = security.require_min_role("viewer")

    user = dependency(UserContext(role="admin", name="admin"))

    assert user == UserContext(role="admin", name="admin")
