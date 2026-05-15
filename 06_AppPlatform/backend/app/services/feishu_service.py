"""Feishu (Lark) OAuth service — code exchange + user identity."""

from __future__ import annotations

import time
from typing import Any

import requests

from app.core.config import FEISHU_APP_ID, FEISHU_APP_SECRET

_TOKEN_URL = (
    "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
)
_OIDC_TOKEN_URL = (
    "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token"
)

# Cache app_access_token (valid for ~2 hours)
_cached_app_token: tuple[str, float] | None = None


def _get_app_access_token() -> str:
    global _cached_app_token
    now = time.time()
    if _cached_app_token and (now - _cached_app_token[1]) < 3600:
        return _cached_app_token[0]

    resp = requests.post(
        _TOKEN_URL,
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["app_access_token"]
    _cached_app_token = (token, now)
    return token


def exchange_code(code: str) -> dict[str, Any]:
    """Exchange an OAuth authorization_code for user identity.

    Returns dict with keys: name, open_id, union_id, email, mobile, avatar_url.
    """
    app_token = _get_app_access_token()
    resp = requests.post(
        _OIDC_TOKEN_URL,
        json={"grant_type": "authorization_code", "code": code},
        headers={"Authorization": f"Bearer {app_token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def build_auth_url(state: str, redirect_uri: str) -> str:
    """Build the Feishu OAuth authorization URL."""
    from urllib.parse import urlencode

    params = {
        "app_id": FEISHU_APP_ID,
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": "openid profile email phone",
    }
    return (
        "https://open.feishu.cn/open-apis/authen/v1/authorize?"
        + urlencode(params)
    )
