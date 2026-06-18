from __future__ import annotations

import hashlib
import json
from typing import Any

from fastapi import Response

from app.core.config import HTTP_STRONG_CACHE_SECONDS


def response_has_cache_control(response: Response) -> bool:
    return any(
        key.lower() == b"cache-control"
        for key, _value in response.raw_headers
    )


def set_strong_json_cache_headers(
    response: Response,
    payload: Any,
    *,
    namespace: str,
) -> None:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:24]
    response.headers["Cache-Control"] = (
        f"public, max-age={max(0, int(HTTP_STRONG_CACHE_SECONDS))}, "
        "stale-while-revalidate=86400"
    )
    response.headers["ETag"] = f'W/"{namespace}-{digest}"'
    response.headers["Vary"] = "Origin"
