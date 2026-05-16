"""Redis client singleton with fail-open behaviour."""
from __future__ import annotations
import logging, threading
from redis import ConnectionPool, Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from app.core.config import REDIS_ENABLED, REDIS_URL
logger = logging.getLogger(__name__)
_client = None
_client_failed = False
_lock = threading.Lock()

def get_redis_client():
    global _client, _client_failed
    if not REDIS_ENABLED or _client_failed:
        return None
    if _client is not None:
        return _client
    with _lock:
        if _client is not None or _client_failed:
            return _client
        try:
            pool = ConnectionPool.from_url(REDIS_URL, max_connections=20, socket_connect_timeout=2, socket_timeout=2, retry_on_timeout=False, decode_responses=False)
            client = Redis(connection_pool=pool)
            client.ping()
            _client = client
            logger.info("Redis connected: %s", REDIS_URL)
            return _client
        except (RedisConnectionError, OSError) as exc:
            _client_failed = True
            logger.warning("Redis unavailable (%s) -- caching disabled: %s", REDIS_URL, exc)
            return None

def reset_redis_client_for_testing():
    global _client, _client_failed
    with _lock:
        if _client is not None:
            try: _client.close()
            except Exception: pass
            _client = None
        _client_failed = False
