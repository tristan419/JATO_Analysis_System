import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from app.api.routes import auth


def _configure_origins(monkeypatch) -> None:
    monkeypatch.setattr(auth, "APP_FRONTEND_ORIGIN", "https://www.ojeur.cloud")
    monkeypatch.setattr(
        auth,
        "FRONTEND_ORIGINS",
        ["https://www.ojeur.cloud", "https://intl.ojeur.cloud"],
    )


def test_frontend_url_uses_allowed_origin(monkeypatch) -> None:
    _configure_origins(monkeypatch)

    url = auth._frontend_url(
        "/account/profile",
        {"token": "abc", "role": "viewer"},
        "https://intl.ojeur.cloud",
    )

    assert url == "https://intl.ojeur.cloud/account/profile?token=abc&role=viewer"


def test_frontend_url_rejects_unlisted_origin(monkeypatch) -> None:
    _configure_origins(monkeypatch)

    url = auth._frontend_url(
        "/login",
        {"oauthError": "failed"},
        "https://evil.example",
    )

    assert url == "https://www.ojeur.cloud/login?oauthError=failed"


def test_frontend_origin_for_request_prefers_allowed_origin_header(monkeypatch) -> None:
    _configure_origins(monkeypatch)
    request = SimpleNamespace(headers={"origin": "https://intl.ojeur.cloud"})

    assert auth._frontend_origin_for_request(request) == "https://intl.ojeur.cloud"


def test_frontend_origin_for_request_falls_back_for_unlisted_origin(monkeypatch) -> None:
    _configure_origins(monkeypatch)
    request = SimpleNamespace(headers={"origin": "https://evil.example"})

    assert auth._frontend_origin_for_request(request) == "https://www.ojeur.cloud"


def test_google_auth_url_preserves_intl_frontend_origin(monkeypatch) -> None:
    _configure_origins(monkeypatch)
    monkeypatch.setattr(auth, "GOOGLE_ENABLED", True)
    monkeypatch.setattr(
        auth,
        "GOOGLE_REDIRECT_URI",
        "https://www.ojeur.cloud/v1/auth/google/callback",
    )
    request = SimpleNamespace(headers={"origin": "https://intl.ojeur.cloud"})

    payload = auth.google_auth_url(request, redirect="/product/order-genius")
    query = parse_qs(urlparse(payload["url"]).query)
    state = json.loads(query["state"][0])

    assert query["redirect_uri"] == [
        "https://www.ojeur.cloud/v1/auth/google/callback"
    ]
    assert state["redirect"] == "/product/order-genius"
    assert state["frontend_origin"] == "https://intl.ojeur.cloud"
