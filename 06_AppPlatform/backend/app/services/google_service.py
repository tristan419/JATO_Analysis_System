"""Google OAuth service — code exchange + user identity."""

from __future__ import annotations

import logging
import os
from urllib.parse import urlencode

import requests

from app.core.config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_OAUTH_RELAY_TOKEN,
    GOOGLE_OAUTH_RELAY_URL,
    GOOGLE_OAUTH_PROXY_URL,
    GOOGLE_OAUTH_TIMEOUT_SECONDS,
)

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
_PROXY_ENV_NAMES = (
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "ALL_PROXY",
    "all_proxy",
)
log = logging.getLogger(__name__)


class GoogleOAuthError(RuntimeError):
    """Raised when Google OAuth cannot complete safely."""


class GoogleOAuthNetworkError(GoogleOAuthError):
    """Raised when the backend cannot reach Google OAuth endpoints."""


def _google_proxy_url() -> str:
    configured = str(GOOGLE_OAUTH_PROXY_URL or "").strip()
    if configured:
        return configured
    for name in _PROXY_ENV_NAMES:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _google_proxies() -> dict[str, str] | None:
    proxy_url = _google_proxy_url()
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def _google_relay_url(path: str) -> str:
    base = str(GOOGLE_OAUTH_RELAY_URL or "").strip().rstrip("/")
    return f"{base}/{path.lstrip('/')}" if base else ""


def _google_relay_headers(headers: dict | None = None) -> dict:
    relay_headers = dict(headers or {})
    relay_headers["X-JATO-Relay-Token"] = GOOGLE_OAUTH_RELAY_TOKEN
    return relay_headers


def _google_request(method: str, url: str, **kwargs) -> requests.Response:
    try:
        return requests.request(
            method,
            url,
            proxies=_google_proxies(),
            timeout=GOOGLE_OAUTH_TIMEOUT_SECONDS,
            **kwargs,
        )
    except requests.exceptions.RequestException as exc:
        log.warning(
            "Google OAuth %s request failed; proxy_configured=%s",
            method.upper(),
            bool(_google_proxy_url()),
            exc_info=True,
        )
        raise GoogleOAuthNetworkError(
            "Google auth failed: backend cannot reach Google OAuth. "
            "Check APP_GOOGLE_OAUTH_PROXY_URL and the local outbound proxy."
        ) from exc


def _google_relay_request(method: str, path: str, **kwargs) -> requests.Response:
    relay_url = _google_relay_url(path)
    if not relay_url:
        raise GoogleOAuthNetworkError("Google OAuth relay is not configured.")
    try:
        headers = _google_relay_headers(kwargs.pop("headers", None))
        return requests.request(
            method,
            relay_url,
            headers=headers,
            timeout=GOOGLE_OAUTH_TIMEOUT_SECONDS,
            **kwargs,
        )
    except requests.exceptions.RequestException as exc:
        log.warning(
            "Google OAuth relay %s request failed; relay_configured=%s",
            method.upper(),
            bool(relay_url),
            exc_info=True,
        )
        raise GoogleOAuthNetworkError(
            "Google auth failed: backend cannot reach the Google OAuth relay."
        ) from exc


def _request_google_token(data: dict) -> requests.Response:
    if _google_relay_url("token"):
        return _google_relay_request("post", "token", data=data)
    return _google_request("post", _TOKEN_URL, data=data)


def _request_google_userinfo(access_token: str) -> requests.Response:
    headers = {"Authorization": f"Bearer {access_token}"}
    if _google_relay_url("userinfo"):
        return _google_relay_request("get", "userinfo", headers=headers)
    return _google_request("get", _USERINFO_URL, headers=headers)


def _raise_for_google_status(resp: requests.Response, step: str) -> None:
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status_code = getattr(resp, "status_code", None)
        log.warning(
            "Google OAuth %s request rejected with status %s",
            step,
            status_code,
            exc_info=True,
        )
        raise GoogleOAuthError(
            f"Google auth failed: Google rejected the {step} request"
            + (f" ({status_code})." if status_code else ".")
        ) from exc


def exchange_code(code: str, redirect_uri: str) -> dict:
    """Exchange authorization_code for tokens + user info.

    Returns dict with: email, name, picture, sub (Google user ID).
    """
    resp = _request_google_token(
        {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }
    )
    _raise_for_google_status(resp, "token")
    tokens = resp.json()
    access_token = tokens.get("access_token")
    if not access_token:
        log.warning("Google OAuth token response did not include access_token")
        raise GoogleOAuthError("Google auth failed: missing access token from Google.")

    user_resp = _request_google_userinfo(access_token)
    _raise_for_google_status(user_resp, "userinfo")
    return user_resp.json()


def build_auth_url(state: str, redirect_uri: str) -> str:
    """Build the Google OAuth authorization URL."""
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
    }
    return (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        + urlencode(params)
    )
