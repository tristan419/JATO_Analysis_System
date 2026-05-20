"""Redis integration for upload toolkit — progress callbacks with TTL keep-alive."""

from typing import Any

from upload_toolkit.upload_engine import ProgressCallback


def make_redis_progress_callback(
    redis_client: Any,
    *,
    key_prefix: str = "upload:session:",
    ttl_seconds: int = 900,
) -> ProgressCallback:
    """Build an on_progress callback that keeps the upload session alive in Redis.

    The caller provides the Redis client (any library with .set()/.expire()
    or .setex() that accepts key, value, ex=…). This way the toolkit never
    hardcodes a host/port.

    Usage:

        import redis
        from upload_toolkit.redis_progress import make_redis_progress_callback

        r = redis.Redis.from_url("redis://tencent-host:6379/0")
        create_upload_session(
            ...,
            on_progress=make_redis_progress_callback(r, ttl_seconds=300),
        )
    """

    def _on_progress(state: dict[str, Any]) -> None:
        upload_id = state.get("uploadId", "")
        if not upload_id:
            return
        key = f"{key_prefix}{upload_id}"
        import json

        try:
            redis_client.setex(key, ttl_seconds, json.dumps(state, ensure_ascii=False))
        except Exception:
            pass  # Redis unavailable — non-fatal

    return _on_progress
