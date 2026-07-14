from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api import msrp_materialization_auth
from app.api.routes import msrp_workflow as msrp_workflow_routes


def test_static_editor_token_with_forged_user_name_cannot_authorize(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_materialization_auth.session_store,
        "lookup",
        lambda _token: None,
    )

    with pytest.raises(HTTPException) as exc_info:
        msrp_materialization_auth.require_materialization_editor_session(
            "static-editor-token"
        )

    assert exc_info.value.status_code == 403
    assert "static service tokens are not accepted" in str(exc_info.value.detail)


def test_authenticated_editor_session_uses_server_side_username(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        msrp_materialization_auth.session_store,
        "lookup",
        lambda token: (
            SimpleNamespace(role="editor", username="real.editor")
            if token == "login-session-token"
            else None
        ),
    )

    actor = msrp_materialization_auth.require_materialization_editor_session(
        "login-session-token"
    )

    assert actor.name == "real.editor"
    assert actor.role == "editor"
    assert actor.identity_source == "authenticated_session"


def test_authenticated_admin_session_is_explicitly_accepted(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_materialization_auth.session_store,
        "lookup",
        lambda _token: SimpleNamespace(role="admin", username="human.admin"),
    )

    actor = msrp_materialization_auth.require_materialization_editor_session(
        "admin-login-session"
    )

    assert actor.name == "human.admin"
    assert actor.role == "admin"


@pytest.mark.parametrize(
    "route_function",
    [
        msrp_workflow_routes.post_materialization_approval,
        msrp_workflow_routes.post_materialize_current_prices,
    ],
)
def test_fact_write_routes_reuse_authenticated_session_dependency(
    route_function,
) -> None:
    user_dependency = inspect.signature(route_function).parameters["user"].default

    assert (
        user_dependency.dependency
        is msrp_materialization_auth.require_materialization_editor_session
    )


@pytest.mark.parametrize("role", ["viewer", "developer", "order_filler"])
def test_non_editor_login_sessions_cannot_authorize(monkeypatch, role: str) -> None:
    monkeypatch.setattr(
        msrp_materialization_auth.session_store,
        "lookup",
        lambda _token: SimpleNamespace(role=role, username="signed.in"),
    )

    with pytest.raises(HTTPException) as exc_info:
        msrp_materialization_auth.require_materialization_editor_session(
            "login-session-token"
        )

    assert exc_info.value.status_code == 403
