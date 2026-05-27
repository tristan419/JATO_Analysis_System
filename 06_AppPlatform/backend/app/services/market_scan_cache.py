"""Redis-backed cache for MarketScan deck responses."""
from __future__ import annotations
import hashlib, json, logging, time
from typing import Any
from redis import Redis
from redis.exceptions import ResponseError
from app.core.config import MARKET_SCAN_CACHE_SCHEMA_VERSION, MARKET_SCAN_CACHE_TTL_SECONDS
logger = logging.getLogger(__name__)
_SCHEMA = MARKET_SCAN_CACHE_SCHEMA_VERSION
_TTL = MARKET_SCAN_CACHE_TTL_SECONDS
_LOCK_TTL = 30
_RETRY_DELAY = 0.2
_MAX_RETRIES = 3

def build_deck_cache_key(country, period, time_range, fuel_types, ranking_limit, dataset_token,
                         trend_window_months=24, origin_window_months=24, body_window_months=24,
                         drilldown_segment=None, view=None):
    tr = ""
    if time_range:
        tr = f"{time_range.get('start','')}:{time_range.get('end','')}"
    fuels = ",".join(sorted(fuel_types))
    token = hashlib.sha256(dataset_token.encode()).hexdigest()[:12] if dataset_token else "notoken"
    ds = drilldown_segment or "none"
    vw = view or "all"
    return f"ms:deck:v{_SCHEMA}:{country}:{period or 'latest'}:{tr or 'default'}:{fuels}:rl{ranking_limit}:tw{trend_window_months}:ow{origin_window_months}:bw{body_window_months}:ds{ds}:vw{vw}:dt{token}"

def get_cached_deck(client, key):
    try:
        raw = client.get(key)
        return json.loads(raw) if raw is not None else None
    except Exception as exc:
        logger.warning("Cache read error %s: %s", key, exc)
        return None

def set_cached_deck(client, key, payload, ttl=None):
    try:
        client.setex(key, ttl or _TTL, json.dumps(payload, ensure_ascii=False, default=str))
        return True
    except Exception as exc:
        logger.warning("Cache write error %s: %s", key, exc)
        return False

def acquire_compute_lock(client, key):
    try:
        ok = client.setnx(f"{key}:lock", "1")
        if ok:
            client.expire(f"{key}:lock", _LOCK_TTL)
        return bool(ok)
    except Exception:
        return True

def release_compute_lock(client, key):
    try: client.delete(f"{key}:lock")
    except Exception: pass

def wait_for_cache(client, key, retries=_MAX_RETRIES, delay=_RETRY_DELAY):
    for _ in range(retries):
        time.sleep(delay)
        c = get_cached_deck(client, key)
        if c is not None:
            return c
    return None

def invalidate_market_scan_deck_cache(
    client: Redis | None,
    *,
    schema_version: int | None = None,
) -> dict[str, Any]:
    schema = schema_version or _SCHEMA
    pattern = f"ms:deck:v{schema}:*"
    if client is None:
        return {
            "enabled": False,
            "pattern": pattern,
            "deletedCount": 0,
            "message": "Redis client unavailable; dataset-token cache keys will expire naturally.",
        }

    deleted_count = 0
    batch_count = 0
    batch: list[Any] = []
    try:
        for key in client.scan_iter(match=pattern, count=250):
            batch.append(key)
            if len(batch) >= 100:
                deleted_count += int(client.delete(*batch) or 0)
                batch_count += 1
                batch = []
        if batch:
            deleted_count += int(client.delete(*batch) or 0)
            batch_count += 1
        return {
            "enabled": True,
            "pattern": pattern,
            "deletedCount": deleted_count,
            "batchCount": batch_count,
        }
    except Exception as exc:
        logger.warning("MarketScan Redis invalidation failed for %s: %s", pattern, exc)
        return {
            "enabled": True,
            "pattern": pattern,
            "deletedCount": deleted_count,
            "batchCount": batch_count,
            "error": str(exc),
            "message": "Redis invalidation failed; dataset-token cache keys will expire naturally.",
        }
