from app.services import google_service


class _JsonResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_exchange_code_uses_configured_proxy(monkeypatch) -> None:
    calls: dict[str, dict] = {}

    def fake_post(url: str, **kwargs) -> _JsonResponse:
        calls["post"] = {"url": url, **kwargs}
        return _JsonResponse({"access_token": "token-1"})

    def fake_get(url: str, **kwargs) -> _JsonResponse:
        calls["get"] = {"url": url, **kwargs}
        return _JsonResponse({"email": "user@example.com", "id": "google-1"})

    monkeypatch.setattr(
        google_service,
        "GOOGLE_OAUTH_PROXY_URL",
        "http://127.0.0.1:7897",
    )
    monkeypatch.setattr(google_service, "GOOGLE_OAUTH_TIMEOUT_SECONDS", 30.0)
    monkeypatch.setattr(google_service.requests, "post", fake_post)
    monkeypatch.setattr(google_service.requests, "get", fake_get)

    result = google_service.exchange_code("code-1", "https://www.ojeur.cloud/v1/auth/google/callback")

    expected_proxies = {
        "http": "http://127.0.0.1:7897",
        "https": "http://127.0.0.1:7897",
    }
    assert result["email"] == "user@example.com"
    assert calls["post"]["proxies"] == expected_proxies
    assert calls["post"]["timeout"] == 30.0
    assert calls["get"]["proxies"] == expected_proxies
    assert calls["get"]["timeout"] == 30.0
