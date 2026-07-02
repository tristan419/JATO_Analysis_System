import pytest

from app.services import google_service


class _JsonResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_exchange_code_uses_configured_proxy(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_request(method: str, url: str, **kwargs) -> _JsonResponse:
        calls.append({"method": method, "url": url, **kwargs})
        if method == "post":
            return _JsonResponse({"access_token": "token-1"})
        return _JsonResponse({"email": "user@example.com", "id": "google-1"})

    monkeypatch.setattr(
        google_service,
        "GOOGLE_OAUTH_PROXY_URL",
        "http://127.0.0.1:7897",
    )
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_RELAY_URL", "")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_TIMEOUT_SECONDS", 30.0)
    monkeypatch.setattr(google_service.requests, "request", fake_request)

    result = google_service.exchange_code(
        "code-1",
        "https://www.ojeur.cloud/v1/auth/google/callback",
    )

    expected_proxies = {
        "http": "http://127.0.0.1:7897",
        "https": "http://127.0.0.1:7897",
    }
    assert result["email"] == "user@example.com"
    assert calls[0]["method"] == "post"
    assert calls[0]["proxies"] == expected_proxies
    assert calls[0]["timeout"] == 30.0
    assert calls[1]["method"] == "get"
    assert calls[1]["proxies"] == expected_proxies
    assert calls[1]["timeout"] == 30.0


def test_exchange_code_uses_standard_proxy_env(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_request(method: str, url: str, **kwargs) -> _JsonResponse:
        calls.append({"method": method, "url": url, **kwargs})
        if method == "post":
            return _JsonResponse({"access_token": "token-1"})
        return _JsonResponse({"email": "user@example.com", "id": "google-1"})

    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_PROXY_URL", "")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_RELAY_URL", "")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("ALL_PROXY", raising=False)
    monkeypatch.setattr(google_service.requests, "request", fake_request)

    google_service.exchange_code(
        "code-1",
        "https://www.ojeur.cloud/v1/auth/google/callback",
    )

    expected_proxies = {
        "http": "http://127.0.0.1:7897",
        "https": "http://127.0.0.1:7897",
    }
    assert calls[0]["proxies"] == expected_proxies
    assert calls[1]["proxies"] == expected_proxies


def test_exchange_code_uses_configured_relay(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_request(method: str, url: str, **kwargs) -> _JsonResponse:
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/token"):
            return _JsonResponse({"access_token": "token-1"})
        return _JsonResponse({"email": "user@example.com", "id": "google-1"})

    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_RELAY_URL", "https://relay.example")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_RELAY_TOKEN", "relay-secret")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_PROXY_URL", "http://127.0.0.1:7897")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_TIMEOUT_SECONDS", 30.0)
    monkeypatch.setattr(google_service.requests, "request", fake_request)

    result = google_service.exchange_code(
        "code-1",
        "https://www.ojeur.cloud/v1/auth/google/callback",
    )

    assert result["email"] == "user@example.com"
    assert calls[0]["method"] == "post"
    assert calls[0]["url"] == "https://relay.example/token"
    assert calls[0]["headers"] == {"X-JATO-Relay-Token": "relay-secret"}
    assert calls[0]["proxies"] == {"http": None, "https": None, "all": None}
    assert calls[1]["method"] == "get"
    assert calls[1]["url"] == "https://relay.example/userinfo"
    assert calls[1]["headers"] == {
        "Authorization": "Bearer token-1",
        "X-JATO-Relay-Token": "relay-secret",
    }
    assert calls[1]["proxies"] == {"http": None, "https": None, "all": None}


def test_exchange_code_wraps_google_network_errors(monkeypatch) -> None:
    def fake_request(method: str, url: str, **kwargs) -> _JsonResponse:
        raise google_service.requests.exceptions.SSLError("EOF")

    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_RELAY_URL", "")
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_PROXY_URL", "")
    for name in google_service._PROXY_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(google_service.requests, "request", fake_request)

    with pytest.raises(google_service.GoogleOAuthNetworkError) as exc_info:
        google_service.exchange_code(
            "code-1",
            "https://www.ojeur.cloud/v1/auth/google/callback",
        )

    assert "backend cannot reach Google OAuth" in str(exc_info.value)
