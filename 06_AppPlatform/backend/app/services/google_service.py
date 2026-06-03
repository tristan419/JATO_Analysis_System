"""Google OAuth service — code exchange + user identity."""

from __future__ import annotations

from urllib.parse import urlencode

import requests

from app.core.config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_OAUTH_PROXY_URL,
    GOOGLE_OAUTH_TIMEOUT_SECONDS,
)

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def _google_proxies() -> dict[str, str] | None:
    if not GOOGLE_OAUTH_PROXY_URL:
        return None
    return {
        "http": GOOGLE_OAUTH_PROXY_URL,
        "https": GOOGLE_OAUTH_PROXY_URL,
    }


def exchange_code(code: str, redirect_uri: str) -> dict:
    """Exchange authorization_code for tokens + user info.

    Returns dict with: email, name, picture, sub (Google user ID).
    """
    resp = requests.post(
        _TOKEN_URL,
        data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
        proxies=_google_proxies(),
        timeout=GOOGLE_OAUTH_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    tokens = resp.json()
    access_token = tokens["access_token"]

    user_resp = requests.get(
        _USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        proxies=_google_proxies(),
        timeout=GOOGLE_OAUTH_TIMEOUT_SECONDS,
    )
    user_resp.raise_for_status()
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
