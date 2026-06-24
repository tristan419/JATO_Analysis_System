from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import pandas as pd
from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from app.db.models import CurrentPrice, HeroProductPriceOverride, HeroProductSpecOverride
from app.db.session import get_session_factory
from app.infra import parquet_repository as repo
from app.infra.redis_client import get_redis_client
from app.services.country_service import country_filter_aliases
from app.services.market_scan_cache import (
    acquire_compute_lock,
    get_cached_deck,
    release_compute_lock,
    set_cached_deck,
    wait_for_cache,
)
from app.services.market_scan_service import (
    _available_periods,
    _coerce_text,
    _country_options,
    _delta_payload,
    _ensure_numeric_columns,
    _normalize_country_lookup,
    _normalize_origin,
    _normalize_overlay_key,
    _normalize_period_range,
    _normalize_powertrain,
    _period_to_month_column,
    _resolve_existing_column,
    _resolve_period,
    _resolve_positioning_sales_window,
    _safe_share,
    _series_sum,
    _short_period_label,
    _shift_period,
    _window_periods,
    _get_columns,
)

logger = logging.getLogger(__name__)

PRICE_SOURCES = ("msrp", "jato")
DEFAULT_SEGMENT = "SUV A0"
DEFAULT_FUEL = "BEV"
DEFAULT_PRICE_CURRENCY = "EUR"
_HERO_PRODUCT_DECK_CACHE_SCHEMA_VERSION = 6
_HERO_PRODUCT_DECK_CACHE_PREFIX = f"ms:hero-product-deck:v{_HERO_PRODUCT_DECK_CACHE_SCHEMA_VERSION}"
_HERO_PRODUCT_DECK_CACHE_TTL_SECONDS = 300
_hero_product_deck_cache: dict[str, tuple[float, str, dict[str, Any]]] = {}
_hero_product_deck_cache_lock = threading.Lock()
SPEC_OVERRIDE_FIELDS = {
    "brand",
    "model",
    "rangeKm",
    "batteryKwh",
    "consumptionKwh100km",
    "accelerationSec",
    "chargingText",
}
CUSTOM_SPEC_FIELD_PREFIX = "custom:"
DEFAULT_HERO_BRANDS = (
    "BYD",
    "OMODA",
    "JAECOO",
    "LYNK",
    "MG",
    "SMART",
    "XPENG",
    "LEAPMOTOR",
)
FUEL_TYPE_DISPLAY_ORDER = (
    "BEV",
    "PHEV",
    "HEV",
    "MHEV",
    "REEV",
    "ICE",
    "FCV",
    "LPG",
    "CNG",
    "OTHER",
)
SPEC_CANDIDATES: dict[str, list[str]] = {
    "rangeKm": [
        "Electric range (km)",
        "EV range (km)",
        "Range (km)",
        "续航",
        "续航(km)",
        "续航 km",
    ],
    "batteryKwh": [
        "Battery capacity (kWh)",
        "Battery kWh",
        "Battery capacity",
        "电池",
        "电池(kWh)",
        "电池 kWh",
    ],
    "consumptionKwh100km": [
        "Electric consumption (kWh/100km)",
        "Energy consumption (kWh/100km)",
        "Consumption kWh/100km",
        "电耗",
        "电耗(kWh/100km)",
    ],
    "accelerationSec": [
        "0-100 km/h (s)",
        "Acceleration 0-100 km/h",
        "0-100km/h",
        "零百加速",
    ],
    "chargingKw": [
        "DC charging power (kW)",
        "Max DC charging (kW)",
        "DC kW",
        "充电功率",
    ],
    "chargingText": [
        "Charging time",
        "DC charging time",
        "Charging technology",
        "充电技术",
        "快充时间",
    ],
}


def _is_supported_spec_override_field(field_name: str) -> bool:
    if field_name in SPEC_OVERRIDE_FIELDS:
        return True
    if not field_name.startswith(CUSTOM_SPEC_FIELD_PREFIX):
        return False
    custom_name = _coerce_text(field_name.removeprefix(CUSTOM_SPEC_FIELD_PREFIX))
    return 0 < len(custom_name) <= 64 and "\x00" not in custom_name


def _normalize_segment_key(value: object) -> str:
    return _coerce_text(value).replace("-", "").replace(" ", "").upper()


def _country_alias_set(*values: str | None) -> set[str]:
    aliases: set[str] = set()
    for value in values:
        text = _coerce_text(value)
        if not text:
            continue
        aliases.add(text.lower())
        try:
            aliases.update(country_filter_aliases(text))
        except Exception:  # noqa: BLE001 - country aliases are convenience only
            pass
    return {alias for alias in aliases if alias}


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _time_range_deck_cache_key(time_range: dict[str, str] | None) -> str:
    if not time_range:
        return ""
    return f"{time_range.get('start', '')}:{time_range.get('end', '')}"


def _normalized_cache_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return [_coerce_text(value) for value in (values or []) if _coerce_text(value)]


def _hero_product_override_token() -> str:
    try:
        session_factory = get_session_factory()
        with session_factory() as session:
            price_updated = session.execute(select(func.max(HeroProductPriceOverride.updated_at_utc))).scalar()
            spec_updated = session.execute(select(func.max(HeroProductSpecOverride.updated_at_utc))).scalar()
            price_count = int(session.execute(select(func.count(HeroProductPriceOverride.override_id))).scalar() or 0)
            spec_count = int(session.execute(select(func.count(HeroProductSpecOverride.override_id))).scalar() or 0)
        price_token = price_updated.isoformat() if price_updated else "none"
        spec_token = spec_updated.isoformat() if spec_updated else "none"
        return f"price={price_token}:{price_count}|spec={spec_token}:{spec_count}"
    except Exception as exc:  # noqa: BLE001 - cache key should not block deck generation
        logger.warning("HeroProduct override token unavailable: %s", exc)
        return "override-token-unavailable"


def _build_hero_product_deck_cache_key(
    *,
    countries: list[str],
    price_country: str | None,
    tracking_country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    sales_mode: str,
    segment: str,
    fuel_types: list[str],
    price_source: str,
    top_n: int,
    ranking_limit: int,
    country_limit: int,
    trend_window_months: int,
    country_rank_scope: str,
    top_models: list[str],
    hero_models: list[str],
    dataset_token: str,
    override_token: str,
) -> str:
    token = hashlib.sha256(dataset_token.encode()).hexdigest()[:12] if dataset_token else "notoken"
    override_hash = hashlib.sha256(override_token.encode()).hexdigest()[:12]
    payload = {
        "countries": sorted(_normalized_cache_list(countries)),
        "priceCountry": _coerce_text(price_country),
        "trackingCountry": _coerce_text(tracking_country),
        "targetPeriod": _coerce_text(target_period) or "latest",
        "timeRange": _time_range_deck_cache_key(time_range),
        "salesMode": _coerce_text(sales_mode),
        "segment": _coerce_text(segment),
        "fuelTypes": sorted(_normalized_cache_list(fuel_types)),
        "priceSource": _coerce_text(price_source),
        "topN": int(top_n),
        "rankingLimit": int(ranking_limit),
        "countryLimit": int(country_limit),
        "trendWindowMonths": int(trend_window_months),
        "countryRankScope": _coerce_text(country_rank_scope),
        "topModels": _normalized_cache_list(top_models),
        "heroModels": _normalized_cache_list(hero_models),
        "dataset": token,
        "overrides": override_hash,
    }
    digest = hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()[:24]
    return f"{_HERO_PRODUCT_DECK_CACHE_PREFIX}:{digest}"


def clear_hero_product_deck_cache() -> dict[str, Any]:
    with _hero_product_deck_cache_lock:
        cleared_count = len(_hero_product_deck_cache)
        _hero_product_deck_cache.clear()
    return {"enabled": True, "clearedCount": cleared_count}


def invalidate_hero_product_deck_cache(client: Any | None) -> dict[str, Any]:
    pattern = f"{_HERO_PRODUCT_DECK_CACHE_PREFIX}:*"
    if client is None:
        return {
            "enabled": False,
            "pattern": pattern,
            "deletedCount": 0,
            "message": "Redis client unavailable; hero product dataset-token cache keys will expire naturally.",
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
    except Exception as exc:  # noqa: BLE001 - cache invalidation should not block publishing/editing
        logger.warning("HeroProduct Redis invalidation failed for %s: %s", pattern, exc)
        return {
            "enabled": True,
            "pattern": pattern,
            "deletedCount": deleted_count,
            "batchCount": batch_count,
            "error": str(exc),
            "message": "HeroProduct Redis invalidation failed; dataset-token cache keys will expire naturally.",
        }


def invalidate_hero_product_runtime_cache() -> dict[str, Any]:
    return {
        "heroProductDeckLocal": clear_hero_product_deck_cache(),
        "heroProductDeckRedis": invalidate_hero_product_deck_cache(get_redis_client()),
    }


def _resolve_country_list(
    requested: list[str],
    options: list[dict[str, str]],
) -> list[dict[str, str]]:
    if not requested:
        return options
    lookup: dict[str, dict[str, str]] = {}
    for option in options:
        lookup[option["value"].strip().lower()] = option
        lookup[option["label"].strip().lower()] = option
    resolved: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in requested:
        option = lookup.get(str(item).strip().lower())
        if not option or option["value"] in seen:
            continue
        resolved.append(option)
        seen.add(option["value"])
    return resolved or options


def _resolve_price_country(
    requested: str | None,
    options: list[dict[str, str]],
) -> dict[str, str]:
    if requested:
        return _normalize_country_lookup(requested, options)
    for option in options:
        if option["value"].strip() == "西班牙":
            return option
        if option["label"].strip().lower() == "spain":
            return option
    return options[0]


def _resolve_tracking_country(
    requested: str | None,
    options: list[dict[str, str]],
    fallback: dict[str, str],
) -> dict[str, str]:
    if requested:
        return _normalize_country_lookup(requested, options)
    return fallback


def _resolve_spec_columns(all_columns: list[str]) -> dict[str, str | None]:
    return {
        key: _resolve_existing_column(candidates, all_columns)
        for key, candidates in SPEC_CANDIDATES.items()
    }


def _normalize_drive_detail(value: object) -> str:
    text = _coerce_text(value).lower()
    if not text or text in {"?", "unknown", "n/a", "na"}:
        return "other"
    if any(token in text for token in ("awd", "4wd", "4x4", "all wheel", "four wheel", "quattro", "4matic", "xdrive")):
        return "4x4"
    if any(token in text for token in ("rwd", "rear wheel", "rear")):
        return "rear"
    if any(token in text for token in ("fwd", "front wheel", "front")):
        return "front"
    return "other"


def _mode_text(series: pd.Series, fallback: str = "") -> str:
    values = [
        _coerce_text(value)
        for value in series.tolist()
        if _coerce_text(value)
    ]
    if not values:
        return fallback
    return pd.Series(values).mode().iloc[0]


def _numeric_values(series: pd.Series, *, limit: int = 3) -> list[float]:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    cleaned = cleaned[cleaned > 0]
    if cleaned.empty:
        return []
    values = sorted({round(float(value), 2) for value in cleaned.tolist()})
    return values[:limit]


def _first_positive(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    values = values[values > 0]
    if values.empty:
        return None
    return float(values.min())


def _text_values(series: pd.Series, *, limit: int = 2) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for value in series.tolist():
        text = _coerce_text(value)
        if not text or text in seen:
            continue
        values.append(text)
        seen.add(text)
        if len(values) >= limit:
            break
    return values


def _model_key(brand: str, model: str) -> tuple[str, str]:
    return (_normalize_overlay_key(brand), _normalize_overlay_key(model))


def _source_brand(row: dict[str, Any]) -> str:
    return str(row.get("sourceBrand") or row.get("brand") or "")


def _source_model(row: dict[str, Any]) -> str:
    return str(row.get("sourceModel") or row.get("model") or "")


def _source_pair(row: dict[str, Any]) -> tuple[str, str]:
    return (_source_brand(row), _source_model(row))


def _source_pairs(model_rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in model_rows:
        pair = _source_pair(row)
        if pair in seen:
            continue
        pairs.append(pair)
        seen.add(pair)
    return pairs


def _filter_frame_for_pairs(frame: pd.DataFrame, pairs: list[tuple[str, str]]) -> pd.DataFrame:
    if frame.empty or not pairs:
        return frame.iloc[0:0]
    pair_index = pd.MultiIndex.from_arrays([
        frame["__brand"].astype(str),
        frame["__model"].astype(str),
    ])
    target_index = pd.MultiIndex.from_tuples(pairs)
    return frame[pair_index.isin(target_index)]


def _segment_filter_candidates(segment: str) -> list[str]:
    text = _coerce_text(segment or DEFAULT_SEGMENT)
    spaced = " ".join(text.replace("-", " ").split())
    upper = spaced.upper()
    candidates = [text, spaced, spaced.title(), upper]
    if upper.startswith("SUV "):
        candidates.append(f"SUV {upper.removeprefix('SUV ')}")
    if upper.startswith("CAR "):
        candidates.append(f"Car {upper.removeprefix('CAR ')}")
    if upper == "SPORTS CAR":
        candidates.extend(["Sports Car", "Sports car", "sports car"])
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _powertrain_filter_candidates(fuel_type: str) -> list[str]:
    text = _coerce_text(fuel_type or DEFAULT_FUEL)
    fuel_key = _normalize_powertrain(text)
    candidates = [text, text.upper(), fuel_key]
    reverse_aliases = {
        "ICE": ["COMBUSTION"],
        "REEV": ["EREV"],
        "FCV": ["FCEV"],
    }
    candidates.extend(reverse_aliases.get(fuel_key, []))
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _fuel_type_sort_key(value: str) -> tuple[int, str]:
    normalized = _normalize_powertrain(value)
    try:
        index = FUEL_TYPE_DISPLAY_ORDER.index(normalized)
    except ValueError:
        index = len(FUEL_TYPE_DISPLAY_ORDER)
    return index, normalized


def _normalize_fuel_type_list(
    fuel_type: str | None,
    fuel_types: list[str] | tuple[str, ...] | None,
) -> list[str]:
    raw_values = list(fuel_types or [])
    if not raw_values:
        raw_values = [fuel_type or DEFAULT_FUEL]
    selected: dict[str, str] = {}
    for raw_value in raw_values:
        text = _coerce_text(raw_value)
        if not text:
            continue
        normalized = _normalize_powertrain(text)
        if not normalized:
            continue
        selected[normalized] = normalized
    if not selected:
        selected[_normalize_powertrain(fuel_type or DEFAULT_FUEL)] = _normalize_powertrain(fuel_type or DEFAULT_FUEL)
    return sorted(selected.values(), key=_fuel_type_sort_key)


def _fuel_type_label(fuel_types: list[str]) -> str:
    values = _normalize_fuel_type_list(DEFAULT_FUEL, fuel_types)
    return " / ".join(values)


def _powertrain_filter_candidates_for_types(fuel_types: list[str]) -> list[str]:
    candidates: list[str] = []
    for fuel_type in fuel_types:
        candidates.extend(_powertrain_filter_candidates(fuel_type))
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


@lru_cache(maxsize=32)
def _available_powertrain_options(dataset_token: str, segment: str) -> list[dict[str, str]]:
    _ = dataset_token
    columns = _get_columns()
    raw_options = repo.load_distinct_options(
        columns.powertrain,
        {columns.segment: _segment_filter_candidates(segment or DEFAULT_SEGMENT)},
    )
    normalized = {
        _normalize_powertrain(option)
        for option in raw_options
        if _coerce_text(option)
    }
    if not normalized:
        normalized.add(DEFAULT_FUEL)
    return [
        {"value": value, "label": value}
        for value in sorted(normalized, key=_fuel_type_sort_key)
    ]


def _table_exists(session: Session, table_name: str, schema: str = "msrp") -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    return inspect(bind).has_table(table_name, schema=schema)


def _load_current_price_map(
    price_country: dict[str, str],
    brands: set[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    try:
        session_factory = get_session_factory()
        session = session_factory()
    except Exception:  # noqa: BLE001 - price DB is optional for read deck
        return {}

    try:
        if not _table_exists(session, "current_prices"):
            return {}
        aliases = _country_alias_set(price_country.get("value"), price_country.get("label"))
        stmt = select(CurrentPrice).where(func.lower(CurrentPrice.country).in_(aliases))
        if brands:
            stmt = stmt.where(CurrentPrice.brand.in_(sorted(brands)))
        rows = session.execute(stmt).scalars().all()
    except Exception:  # noqa: BLE001
        return {}
    finally:
        session.close()

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        price = float(row.current_msrp_value or 0)
        if price <= 0:
            continue
        for model_name in (row.jato_model, row.official_model):
            key = _model_key(row.brand, model_name)
            if not key[0] or not key[1]:
                continue
            existing = result.get(key)
            if existing is None or price < float(existing["value"]):
                result[key] = {
                    "value": price,
                    "currency": row.currency or DEFAULT_PRICE_CURRENCY,
                    "updatedAt": row.updated_at_utc.isoformat() if row.updated_at_utc else None,
                    "sourceUrl": row.source_url,
                }
    return result


def _load_price_overrides(
    price_country: dict[str, str],
    period: str,
) -> dict[tuple[str, str, str], HeroProductPriceOverride]:
    try:
        session_factory = get_session_factory()
        session = session_factory()
    except Exception:  # noqa: BLE001 - overrides are optional for read deck
        return {}

    try:
        if not _table_exists(session, "hero_product_price_overrides"):
            return {}
        aliases = _country_alias_set(price_country.get("value"), price_country.get("label"))
        stmt = (
            select(HeroProductPriceOverride)
            .where(func.lower(HeroProductPriceOverride.country).in_(aliases))
            .where(HeroProductPriceOverride.price_source.in_(PRICE_SOURCES))
            .where(HeroProductPriceOverride.price_period.in_([period, ""]))
            .order_by(HeroProductPriceOverride.price_period.asc())
        )
        rows = session.execute(stmt).scalars().all()
    except Exception:  # noqa: BLE001
        return {}
    finally:
        session.close()

    result: dict[tuple[str, str, str], HeroProductPriceOverride] = {}
    for row in rows:
        brand_key, model_key = _model_key(row.brand, row.model)
        if not brand_key or not model_key:
            continue
        result[(row.price_source, brand_key, model_key)] = row
    return result


def _load_spec_overrides(
    price_country: dict[str, str],
    period: str,
) -> dict[tuple[str, str], dict[str, HeroProductSpecOverride]]:
    try:
        session_factory = get_session_factory()
        session = session_factory()
    except Exception:  # noqa: BLE001 - overrides are optional for read deck
        return {}

    try:
        if not _table_exists(session, "hero_product_spec_overrides"):
            return {}
        aliases = _country_alias_set(price_country.get("value"), price_country.get("label"))
        stmt = (
            select(HeroProductSpecOverride)
            .where(func.lower(HeroProductSpecOverride.country).in_(aliases))
            .where(HeroProductSpecOverride.price_period.in_([period, ""]))
            .order_by(HeroProductSpecOverride.price_period.asc())
        )
        rows = session.execute(stmt).scalars().all()
    except Exception:  # noqa: BLE001
        return {}
    finally:
        session.close()

    result: dict[tuple[str, str], dict[str, HeroProductSpecOverride]] = {}
    for row in rows:
        brand_key, model_key = _model_key(row.brand, row.model)
        if not brand_key or not model_key or not _is_supported_spec_override_field(row.field_name):
            continue
        result.setdefault((brand_key, model_key), {})[row.field_name] = row
    return result


def _override_payload(row: HeroProductPriceOverride | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "overrideId": str(row.override_id),
        "value": float(row.price_value),
        "currency": row.currency,
        "updatedBy": row.updated_by,
        "updatedAt": row.updated_at_utc.isoformat() if row.updated_at_utc else None,
        "note": row.note,
    }


def _price_source_payload(
    *,
    source: str,
    raw_value: float | None,
    raw_currency: str,
    override: HeroProductPriceOverride | None,
) -> dict[str, Any]:
    override_data = _override_payload(override)
    value = override_data["value"] if override_data else raw_value
    return {
        "source": source,
        "value": value,
        "rawValue": raw_value,
        "currency": override_data["currency"] if override_data else raw_currency,
        "status": "manual_override" if override_data else ("available" if raw_value is not None else "missing"),
        "override": override_data,
    }


def _build_price_payload(
    *,
    brand: str,
    model: str,
    jato_value: float | None,
    msrp_entry: dict[str, Any] | None,
    overrides: dict[tuple[str, str, str], HeroProductPriceOverride],
    selected_source: str,
) -> dict[str, Any]:
    brand_key, model_key = _model_key(brand, model)
    jato_payload = _price_source_payload(
        source="jato",
        raw_value=jato_value,
        raw_currency=DEFAULT_PRICE_CURRENCY,
        override=overrides.get(("jato", brand_key, model_key)),
    )
    msrp_payload = _price_source_payload(
        source="msrp",
        raw_value=float(msrp_entry["value"]) if msrp_entry else None,
        raw_currency=str(msrp_entry.get("currency") or DEFAULT_PRICE_CURRENCY) if msrp_entry else DEFAULT_PRICE_CURRENCY,
        override=overrides.get(("msrp", brand_key, model_key)),
    )
    return {
        "selectedSource": selected_source,
        "selected": msrp_payload if selected_source == "msrp" else jato_payload,
        "sources": {
            "msrp": msrp_payload,
            "jato": jato_payload,
        },
    }


def _time_range_cache_key(time_range: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not time_range:
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in time_range.items()))


def _build_source_frame(
    *,
    countries: list[str],
    price_country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    sales_mode: str,
    segment: str,
    fuel_types: list[str],
    trend_window_months: int,
) -> dict[str, Any]:
    selected_fuel_types = _normalize_fuel_type_list(DEFAULT_FUEL, fuel_types)
    base = _build_source_base_frame_cached(
        repo.current_dataset_token(),
        tuple(countries),
        price_country or "",
        target_period or "",
        _time_range_cache_key(time_range),
        segment or "",
        tuple(selected_fuel_types),
        int(trend_window_months),
    )
    return _derive_source_frame_for_sales_window(base, sales_mode or "")


@lru_cache(maxsize=16)
def _build_source_base_frame_cached(
    dataset_token: str,
    countries_key: tuple[str, ...],
    price_country: str,
    target_period: str,
    time_range_key: tuple[tuple[str, str], ...],
    segment: str,
    fuel_types_key: tuple[str, ...],
    trend_window_months: int,
) -> dict[str, Any]:
    time_range = dict(time_range_key) if time_range_key else None
    columns = _get_columns()
    all_columns = repo.list_columns()
    spec_columns = _resolve_spec_columns(all_columns)
    available_periods = _available_periods(columns)
    resolved_period = _resolve_period(target_period or None, available_periods)
    custom_periods = _normalize_period_range(available_periods, time_range, resolved_period)
    trend_periods = _window_periods(
        available_periods,
        resolved_period,
        trend_window_months,
    )
    read_periods: list[str] = []
    for mode in ("month", "ytd", "rolling12"):
        sales_periods, _, _ = _resolve_positioning_sales_window(
            available_periods,
            resolved_period,
            mode,
            custom_periods,
        )
        read_periods.extend(sales_periods)
        read_periods.extend(
            shifted
            for period in sales_periods
            if (shifted := _shift_period(period, -12)) in available_periods
        )
    read_periods.extend(trend_periods)
    month_columns = [
        _period_to_month_column(period)
        for period in list(dict.fromkeys(read_periods))
    ]

    country_options = _country_options(dataset_token)
    selected_countries = _resolve_country_list(list(countries_key), country_options)
    selected_price_country = _resolve_price_country(price_country or None, country_options)
    selected_country_values = [item["value"] for item in selected_countries]
    segment_key = _normalize_segment_key(segment or DEFAULT_SEGMENT)
    selected_fuel_types = _normalize_fuel_type_list(DEFAULT_FUEL, fuel_types_key)
    selected_fuel_set = set(selected_fuel_types)
    segment_filter_values = _segment_filter_candidates(segment or DEFAULT_SEGMENT)
    powertrain_filter_values = _powertrain_filter_candidates_for_types(selected_fuel_types)

    selected_columns = [
        columns.country_value,
        columns.make,
        columns.model,
        columns.segment,
        columns.powertrain,
        *month_columns,
    ]
    for optional_column in (
        columns.country_label,
        columns.length,
        columns.msrp,
        columns.version,
        columns.trim,
        columns.drive_type,
        columns.registration_type,
        columns.origin,
        *spec_columns.values(),
    ):
        if optional_column:
            selected_columns.append(optional_column)
    selected_columns = list(dict.fromkeys(selected_columns))

    dataset = repo._open_dataset()
    pushdown_filters: dict[str, list[str]] = {
        columns.segment: segment_filter_values,
        columns.powertrain: powertrain_filter_values,
    }
    if 0 < len(selected_country_values) < len(country_options):
        pushdown_filters[columns.country_value] = selected_country_values
    filter_expression = repo._build_filter_expression(pushdown_filters)
    table = (
        dataset.to_table(columns=selected_columns, filter=filter_expression)
        if filter_expression is not None
        else dataset.to_table(columns=selected_columns)
    )
    frame = table.to_pandas()
    numeric_columns = [
        *month_columns,
        *(column for column in (columns.length, columns.msrp) if column),
        *(column for key, column in spec_columns.items() if column and key != "chargingText"),
    ]
    frame = _ensure_numeric_columns(frame, numeric_columns)
    frame["__country_value"] = frame[columns.country_value].astype(str).str.strip()
    frame["__country_label"] = (
        frame[columns.country_label].astype(str).str.strip()
        if columns.country_label and columns.country_label in frame.columns
        else frame["__country_value"]
    )
    frame["__brand"] = frame[columns.make].astype(str).str.strip()
    frame["__model"] = frame[columns.model].astype(str).str.strip()
    frame["__segment_raw"] = frame[columns.segment].astype(str).str.strip()
    frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
    frame["__origin"] = (
        frame[columns.origin].map(_normalize_origin)
        if columns.origin and columns.origin in frame.columns
        else "其他"
    )
    frame["__trim"] = (
        frame[columns.trim].astype(str).str.strip()
        if columns.trim and columns.trim in frame.columns
        else ""
    )
    frame["__version"] = (
        frame[columns.version].astype(str).str.strip()
        if columns.version and columns.version in frame.columns
        else frame["__trim"]
    )
    frame["__drive_detail"] = (
        frame[columns.drive_type].map(_normalize_drive_detail)
        if columns.drive_type and columns.drive_type in frame.columns
        else "other"
    )
    frame["__registration_type"] = (
        frame[columns.registration_type].astype(str).str.strip()
        if columns.registration_type and columns.registration_type in frame.columns
        else "Other"
    )
    frame["__length"] = (
        pd.to_numeric(frame[columns.length], errors="coerce").fillna(0.0)
        if columns.length and columns.length in frame.columns
        else 0.0
    )
    frame["__msrp_jato"] = (
        pd.to_numeric(frame[columns.msrp], errors="coerce").fillna(0.0)
        if columns.msrp and columns.msrp in frame.columns
        else 0.0
    )

    frame["__available_sales"] = _series_sum(frame, month_columns)
    frame = frame[
        (frame["__brand"] != "")
        & (frame["__model"] != "")
        & (frame["__available_sales"] > 0)
        & (frame["__segment_raw"].map(_normalize_segment_key) == segment_key)
        & (frame["__powertrain"].isin(selected_fuel_set))
    ].copy()

    return {
        "columns": columns,
        "specColumns": spec_columns,
        "baseFrame": frame,
        "countryOptions": country_options,
        "selectedCountries": selected_countries,
        "priceCountry": selected_price_country,
        "availablePeriods": available_periods,
        "resolvedPeriod": resolved_period,
        "latestPeriod": available_periods[-1],
        "trendPeriods": trend_periods,
        "customPeriods": custom_periods,
        "selectedSegment": segment or DEFAULT_SEGMENT,
        "selectedFuelTypes": selected_fuel_types,
        "selectedFuelType": _fuel_type_label(selected_fuel_types),
        "availableFuelTypes": _available_powertrain_options(dataset_token, segment or DEFAULT_SEGMENT),
    }


def _derive_source_frame_for_sales_window(base: dict[str, Any], sales_mode: str) -> dict[str, Any]:
    available_periods: list[str] = base["availablePeriods"]
    resolved_period: str = base["resolvedPeriod"]
    custom_periods: list[str] | None = base["customPeriods"]
    sales_periods, sales_mode_label, sales_metric_label = _resolve_positioning_sales_window(
        available_periods,
        resolved_period,
        sales_mode,
        custom_periods,
    )
    prior_sales_periods = [
        shifted
        for period in sales_periods
        if (shifted := _shift_period(period, -12)) in available_periods
    ]
    sales_columns = [_period_to_month_column(period) for period in sales_periods]
    prior_sales_columns = [_period_to_month_column(period) for period in prior_sales_periods]
    frame = base["baseFrame"].copy()
    frame["__sales"] = _series_sum(frame, sales_columns)
    frame["__prior_sales"] = _series_sum(frame, prior_sales_columns)
    frame = frame[(frame["__sales"] > 0) | (frame["__prior_sales"] > 0)].copy()
    price_frame = frame[
        (frame["__country_value"] == base["priceCountry"]["value"])
        & (frame["__sales"] > 0)
    ].copy()
    return {
        **base,
        "frame": frame,
        "priceFrame": price_frame,
        "salesPeriods": sales_periods,
        "priorSalesPeriods": prior_sales_periods,
        "salesModeLabel": sales_mode_label,
        "salesMetricLabel": sales_metric_label,
    }


def _row_specs(group: pd.DataFrame, spec_columns: dict[str, str | None]) -> dict[str, Any]:
    specs: dict[str, Any] = {
        "lengthMm": _first_positive(group["__length"]) if "__length" in group.columns else None,
        "rangeKm": [],
        "batteryKwh": [],
        "consumptionKwh100km": [],
        "accelerationSec": [],
        "chargingKw": [],
        "chargingText": [],
    }
    for key, column in spec_columns.items():
        if not column or column not in group.columns:
            continue
        if key == "chargingText":
            specs[key] = _text_values(group[column])
        else:
            specs[key] = _numeric_values(group[column])
    return specs


def _parse_numeric_list_text(value: str) -> list[float]:
    normalized = value.replace("，", "/").replace(",", "/").replace("|", "/")
    values: list[float] = []
    seen: set[float] = set()
    for part in normalized.split("/"):
        text = part.strip().replace("km", "").replace("kWh", "").replace("kW", "").replace("s", "")
        if not text or text == "-":
            continue
        try:
            numeric = round(float(text), 2)
        except ValueError:
            continue
        if numeric in seen:
            continue
        values.append(numeric)
        seen.add(numeric)
    return values


def _parse_text_list(value: str) -> list[str]:
    normalized = value.replace("，", "/").replace(",", "/").replace("|", "/")
    result: list[str] = []
    seen: set[str] = set()
    for part in normalized.split("/"):
        text = _coerce_text(part)
        if not text or text == "-" or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result


def _apply_spec_overrides(
    row: dict[str, Any],
    spec_overrides: dict[tuple[str, str], dict[str, HeroProductSpecOverride]],
) -> dict[str, Any]:
    brand_key, model_key = _model_key(row["brand"], row["model"])
    overrides = spec_overrides.get((brand_key, model_key), {})
    if not overrides:
        row["sourceBrand"] = row["brand"]
        row["sourceModel"] = row["model"]
        return row

    row["sourceBrand"] = row["brand"]
    row["sourceModel"] = row["model"]
    spec_payload = dict(row["specs"])
    override_payload: dict[str, dict[str, Any]] = {}
    for field, override in overrides.items():
        text = _coerce_text(override.field_value)
        override_payload[field] = {
            "overrideId": str(override.override_id),
            "value": text,
            "updatedBy": override.updated_by,
            "updatedAt": override.updated_at_utc.isoformat() if override.updated_at_utc else None,
            "note": override.note,
        }
        if field == "brand":
            row["brand"] = text or row["brand"]
        elif field == "model":
            row["model"] = text or row["model"]
        elif field == "chargingText":
            spec_payload["chargingText"] = _parse_text_list(text)
            spec_payload["chargingKw"] = []
        elif field in SPEC_OVERRIDE_FIELDS:
            spec_payload[field] = _parse_numeric_list_text(text)
    spec_payload["overrides"] = override_payload
    row["specs"] = spec_payload
    row["label"] = f"{row['brand']} {row['model']}".strip()
    return row


def _mix_payload(group: pd.DataFrame, column: str, categories: tuple[str, ...]) -> dict[str, float]:
    if group.empty or column not in group.columns:
        return {category: 0.0 for category in categories}
    grouped = group.groupby(column, dropna=False)["__sales"].sum()
    return {
        category: float(grouped.get(category, 0.0))
        for category in categories
    }


def _aggregate_models(
    frame: pd.DataFrame,
    price_frame: pd.DataFrame,
    spec_columns: dict[str, str | None],
    current_price_map: dict[tuple[str, str], dict[str, Any]],
    overrides: dict[tuple[str, str, str], HeroProductPriceOverride],
    spec_overrides: dict[tuple[str, str], dict[str, HeroProductSpecOverride]],
    price_source: str,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []

    rows: list[dict[str, Any]] = []
    grouped = frame.groupby(["__brand", "__model"], dropna=False, sort=False)
    price_grouped = (
        price_frame.groupby(["__brand", "__model"], dropna=False, sort=False)
        if not price_frame.empty
        else None
    )
    for (brand, model), group in grouped:
        brand_text = str(brand)
        model_text = str(model)
        active_group = group[group["__sales"] > 0]
        sales = float(active_group["__sales"].sum())
        if sales <= 0:
            continue
        try:
            spec_group = price_grouped.get_group((brand, model)) if price_grouped is not None else group
        except KeyError:
            spec_group = active_group
        if spec_group.empty:
            spec_group = active_group
        jato_price = _first_positive(spec_group["__msrp_jato"]) if "__msrp_jato" in spec_group.columns else None
        current_price_entry = current_price_map.get(_model_key(brand_text, model_text))
        prior_sales = float(group["__prior_sales"].sum())
        channel_mix = _mix_payload(active_group, "__registration_type", ("Business", "Private", "Other"))
        channel_total = sum(channel_mix.values()) or sales
        drive_mix = _mix_payload(active_group, "__drive_detail", ("front", "rear", "4x4", "other"))
        row = {
            "brand": brand_text,
            "model": model_text,
            "label": f"{brand_text} {model_text}".strip(),
            "origin": _mode_text(active_group["__origin"], "其他"),
            "sales": sales,
            "priorSales": prior_sales,
            "yoy": _delta_payload(sales, prior_sales),
            "sharePct": 0.0,
            "barPct": 0.0,
            "channelMix": channel_mix,
            "channelSharePct": {
                channel: _safe_share(value, channel_total)
                for channel, value in channel_mix.items()
            },
            "driveMix": drive_mix,
            "specs": _row_specs(spec_group, spec_columns),
            "price": _build_price_payload(
                brand=brand_text,
                model=model_text,
                jato_value=jato_price,
                msrp_entry=current_price_entry,
                overrides=overrides,
                selected_source=price_source,
            ),
        }
        row = _apply_spec_overrides(row, spec_overrides)
        rows.append(row)

    rows.sort(key=lambda item: (-float(item["sales"]), item["brand"], item["model"]))
    total_sales = sum(float(item["sales"]) for item in rows)
    max_sales = max((float(item["sales"]) for item in rows), default=1.0)
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
        row["sharePct"] = _safe_share(float(row["sales"]), total_sales)
        row["barPct"] = _safe_share(float(row["sales"]), max_sales)
    return rows

def _resolve_requested_models(
    model_rows: list[dict[str, Any]],
    requested: list[str],
    fallback: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    if not requested:
        return fallback[:limit]
    lookup: dict[str, dict[str, Any]] = {}
    for row in model_rows:
        lookup[_normalize_overlay_key(str(row["model"]))] = row
        lookup[_normalize_overlay_key(str(row["label"]))] = row
        lookup[_normalize_overlay_key(_source_model(row))] = row
        lookup[_normalize_overlay_key(f"{_source_brand(row)} {_source_model(row)}")] = row
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in requested:
        row = lookup.get(_normalize_overlay_key(item))
        if not row:
            continue
        key = str(row["label"])
        if key in seen:
            continue
        selected.append(row)
        seen.add(key)
        if len(selected) >= limit:
            break
    return selected or fallback[:limit]


def _default_hero_rows(model_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    hero = [
        row
        for row in model_rows
        if str(row.get("origin")) == "中系"
        or any(token in str(row.get("brand", "")).upper() for token in DEFAULT_HERO_BRANDS)
        or any(token in str(row.get("model", "")).upper() for token in DEFAULT_HERO_BRANDS)
    ]
    return (hero or model_rows)[:limit]


def _build_trend_series(
    frame: pd.DataFrame,
    model_rows: list[dict[str, Any]],
    trend_periods: list[str],
) -> list[dict[str, Any]]:
    pairs = _source_pairs(model_rows)
    period_columns = [
        column
        for column in dict.fromkeys(_period_to_month_column(period) for period in trend_periods)
        if column in frame.columns
    ]
    grouped = pd.DataFrame()
    if pairs and period_columns and not frame.empty:
        working = _filter_frame_for_pairs(frame, pairs)
        if not working.empty:
            grouped = working.groupby(["__brand", "__model"], dropna=False, sort=False)[period_columns].sum()

    series: list[dict[str, Any]] = []
    for row in model_rows:
        source_brand, source_model = _source_pair(row)
        values = None
        if not grouped.empty:
            try:
                values = grouped.loc[(source_brand, source_model)]
            except KeyError:
                values = None
        points: list[dict[str, Any]] = []
        for period in trend_periods:
            column = _period_to_month_column(period)
            volume = float(values.get(column, 0.0)) if values is not None and column in period_columns else 0.0
            points.append({
                "period": period,
                "label": _short_period_label(period),
                "volume": volume,
            })
        series.append({
            "brand": row["brand"],
            "model": row["model"],
            "label": row["model"],
            "sourceBrand": source_brand,
            "sourceModel": source_model,
            "points": points,
        })
    return series


def _build_country_ranking_trace(
    frame: pd.DataFrame,
    model_rows: list[dict[str, Any]],
    trend_periods: list[str],
    tracking_country: dict[str, str],
    rank_window: int,
) -> dict[str, Any]:
    periods = [
        {"period": period, "label": _short_period_label(period)}
        for period in trend_periods
    ]
    if frame.empty or not model_rows:
        return {
            "country": tracking_country,
            "rankWindow": max(1, int(rank_window or 20)),
            "periods": periods,
            "series": [],
        }

    country_frame = frame[frame["__country_value"] == tracking_country["value"]].copy()
    selected_pairs = {_source_pair(row) for row in model_rows}
    if country_frame.empty:
        return {
            "country": tracking_country,
            "rankWindow": max(1, int(rank_window or 20)),
            "periods": periods,
            "series": [
                {
                    "brand": row["brand"],
                    "model": row["model"],
                    "label": row["model"],
                    "sourceBrand": _source_brand(row),
                    "sourceModel": _source_model(row),
                    "points": [
                        {
                            "period": period["period"],
                            "label": period["label"],
                            "rank": None,
                            "sales": 0.0,
                            "sharePct": 0.0,
                            "inWindow": False,
                        }
                        for period in periods
                    ],
                }
                for row in model_rows
            ],
        }

    ranking_by_period: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    for period in trend_periods:
        column = _period_to_month_column(period)
        if column not in country_frame.columns:
            ranking_by_period[period] = {}
            continue
        grouped = (
            country_frame.groupby(["__brand", "__model"], dropna=False)[column]
            .sum()
            .reset_index(name="sales")
        )
        grouped["sales"] = pd.to_numeric(grouped["sales"], errors="coerce").fillna(0.0)
        grouped = grouped[grouped["sales"] > 0].sort_values(
            ["sales", "__brand", "__model"],
            ascending=[False, True, True],
        )
        total = float(grouped["sales"].sum())
        ranking_by_period[period] = {
            (str(row["__brand"]), str(row["__model"])): {
                "rank": index,
                "sales": float(row["sales"]),
                "sharePct": _safe_share(float(row["sales"]), total),
            }
            for index, row in enumerate(grouped.to_dict("records"), start=1)
        }

    result_series: list[dict[str, Any]] = []
    for row in model_rows:
        source_brand, source_model = _source_pair(row)
        pair = (source_brand, source_model)
        if pair not in selected_pairs:
            continue
        points: list[dict[str, Any]] = []
        for period in periods:
            item = ranking_by_period.get(period["period"], {}).get(pair)
            rank = int(item["rank"]) if item else None
            points.append({
                "period": period["period"],
                "label": period["label"],
                "rank": rank,
                "sales": float(item["sales"]) if item else 0.0,
                "sharePct": float(item["sharePct"]) if item else 0.0,
                "inWindow": bool(rank is not None and rank <= max(1, int(rank_window or 20))),
            })
        result_series.append({
            "brand": row["brand"],
            "model": row["model"],
            "label": row["model"],
            "sourceBrand": source_brand,
            "sourceModel": source_model,
            "points": points,
        })

    return {
        "country": tracking_country,
        "rankWindow": max(1, int(rank_window or 20)),
        "periods": periods,
        "series": result_series,
    }


def _country_rank_trace_options(
    country_labels: list[str],
    country_options: list[dict[str, str]],
    selected_country: dict[str, str],
) -> list[dict[str, str]]:
    resolved: list[dict[str, str]] = []
    seen: set[str] = set()
    for country in [selected_country["value"], selected_country["label"], *country_labels]:
        option = _normalize_country_lookup(country, country_options)
        key = option["value"]
        if key in seen:
            continue
        resolved.append(option)
        seen.add(key)
    return resolved


def _build_country_ranking_trace_map(
    frame: pd.DataFrame,
    model_rows: list[dict[str, Any]],
    trend_periods: list[str],
    countries: list[dict[str, str]],
    rank_window: int,
) -> dict[str, Any]:
    periods = [
        {"period": period, "label": _short_period_label(period)}
        for period in trend_periods
    ]
    rank_limit = max(1, int(rank_window or 20))
    if frame.empty or not model_rows:
        return {
            country["label"] or country["value"]: {
                "country": country,
                "rankWindow": rank_limit,
                "periods": periods,
                "series": [],
            }
            for country in countries
        }

    country_values = {str(country["value"]) for country in countries if str(country.get("value") or "").strip()}
    ranking_frame = (
        frame[frame["__country_value"].astype(str).isin(country_values)].copy()
        if country_values
        else frame
    )

    rankings_by_country: dict[str, dict[str, dict[tuple[str, str], dict[str, Any]]]] = {}
    for period in trend_periods:
        column = _period_to_month_column(period)
        if column not in ranking_frame.columns:
            continue
        grouped = (
            ranking_frame.groupby(["__country_value", "__brand", "__model"], dropna=False)[column]
            .sum()
            .reset_index(name="sales")
        )
        grouped["sales"] = pd.to_numeric(grouped["sales"], errors="coerce").fillna(0.0)
        grouped = grouped[grouped["sales"] > 0]
        if grouped.empty:
            continue
        for country_value, country_group in grouped.groupby("__country_value", dropna=False):
            ranked = country_group.sort_values(
                ["sales", "__brand", "__model"],
                ascending=[False, True, True],
            )
            total = float(ranked["sales"].sum())
            rankings_by_country.setdefault(str(country_value), {})[period] = {
                (str(row["__brand"]), str(row["__model"])): {
                    "rank": index,
                    "sales": float(row["sales"]),
                    "sharePct": _safe_share(float(row["sales"]), total),
                }
                for index, row in enumerate(ranked.to_dict("records"), start=1)
            }

    result: dict[str, Any] = {}
    for country in countries:
        key = country["label"] or country["value"]
        country_rankings = rankings_by_country.get(country["value"], {})
        series: list[dict[str, Any]] = []
        for row in model_rows:
            source_brand, source_model = _source_pair(row)
            pair = (source_brand, source_model)
            points: list[dict[str, Any]] = []
            for period in periods:
                item = country_rankings.get(period["period"], {}).get(pair)
                rank = int(item["rank"]) if item else None
                points.append({
                    "period": period["period"],
                    "label": period["label"],
                    "rank": rank,
                    "sales": float(item["sales"]) if item else 0.0,
                    "sharePct": float(item["sharePct"]) if item else 0.0,
                    "inWindow": bool(rank is not None and rank <= rank_limit),
                })
            series.append({
                "brand": row["brand"],
                "model": row["model"],
                "label": row["model"],
                "sourceBrand": source_brand,
                "sourceModel": source_model,
                "points": points,
            })
        result[key] = {
            "country": country,
            "rankWindow": rank_limit,
            "periods": periods,
            "series": series,
        }
    return result


def _filter_country_ranking_trace_map(
    trace_map: dict[str, Any],
    model_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_pairs = set(_source_pairs(model_rows))
    result: dict[str, Any] = {}
    for key, payload in trace_map.items():
        filtered_series = []
        for series in payload.get("series", []):
            pair = (
                str(series.get("sourceBrand") or series.get("brand") or ""),
                str(series.get("sourceModel") or series.get("model") or ""),
            )
            if pair in selected_pairs:
                filtered_series.append(series)
        result[key] = {
            **payload,
            "series": filtered_series,
        }
    return result


def _build_country_distribution(
    frame: pd.DataFrame,
    model_rows: list[dict[str, Any]],
    country_limit: int,
) -> dict[str, Any]:
    if frame.empty or not model_rows:
        return {"countries": [], "items": []}
    pairs = _source_pairs(model_rows)
    working = _filter_frame_for_pairs(frame, pairs).copy()
    working = working[working["__sales"] > 0].copy()
    if working.empty:
        return {"countries": [], "items": []}
    country_totals = (
        working.groupby("__country_label")["__sales"]
        .sum()
        .sort_values(ascending=False)
    )
    all_countries = [str(country) for country in country_totals.index if float(country_totals.loc[country]) > 0]
    max_countries = max(0, int(country_limit or 0))
    drive_grouped = (
        working.groupby(["__brand", "__model", "__country_label", "__drive_detail"], dropna=False, sort=False)["__sales"]
        .sum()
        .reset_index(name="sales")
    )
    country_grouped = (
        drive_grouped.groupby(["__brand", "__model", "__country_label"], dropna=False, sort=False)["sales"]
        .sum()
        .reset_index(name="sales")
        .sort_values(["__brand", "__model", "sales", "__country_label"], ascending=[True, True, False, True])
    )
    countries_by_pair: dict[tuple[str, str], list[tuple[str, float]]] = {}
    for item in country_grouped.to_dict("records"):
        pair = (str(item["__brand"]), str(item["__model"]))
        countries_by_pair.setdefault(pair, []).append((str(item["__country_label"]), float(item["sales"])))
    drive_by_pair_country: dict[tuple[str, str, str], dict[str, float]] = {}
    for item in drive_grouped.to_dict("records"):
        key = (str(item["__brand"]), str(item["__model"]), str(item["__country_label"]))
        drive_by_pair_country.setdefault(key, {"front": 0.0, "rear": 0.0, "4x4": 0.0, "other": 0.0})[
            str(item["__drive_detail"])
        ] = float(item["sales"])

    items: list[dict[str, Any]] = []
    for row in model_rows:
        source_brand, source_model = _source_pair(row)
        countries = countries_by_pair.get((source_brand, source_model), [])
        if max_countries > 0:
            countries = countries[:max_countries]
        country_items: list[dict[str, Any]] = []
        for country, total in countries:
            country_items.append({
                "country": country,
                "sales": total,
                "driveMix": drive_by_pair_country.get(
                    (source_brand, source_model, country),
                    {"front": 0.0, "rear": 0.0, "4x4": 0.0, "other": 0.0},
                ),
            })
        items.append({
            "brand": row["brand"],
            "model": row["model"],
            "sourceBrand": source_brand,
            "sourceModel": source_model,
            "totalSales": float(row["sales"]),
            "countries": country_items,
        })
    displayed_countries = {
        country["country"]
        for item in items
        for country in item["countries"]
    }
    return {
        "countries": [country for country in all_countries if country in displayed_countries],
        "items": items,
    }


def _build_price_panel_rows(model_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "brand": row["brand"],
            "model": row["model"],
            "sourceBrand": _source_brand(row),
            "sourceModel": _source_model(row),
            "origin": row["origin"],
            "price": row["price"],
        }
        for row in model_rows
    ]


def query_hero_product_deck(
    *,
    countries: list[str],
    price_country: str | None,
    tracking_country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    sales_mode: str,
    segment: str,
    fuel_type: str,
    fuel_types: list[str] | None = None,
    price_source: str,
    top_n: int,
    ranking_limit: int,
    country_limit: int,
    trend_window_months: int,
    country_rank_scope: str,
    top_models: list[str],
    hero_models: list[str],
) -> dict[str, Any]:
    dataset_token = repo.current_dataset_token()
    override_token = _hero_product_override_token()
    normalized_fuel_types = _normalize_fuel_type_list(fuel_type, fuel_types)
    normalized_top_n = min(30, max(10, int(top_n or 10)))
    normalized_ranking_limit = min(60, max(normalized_top_n, int(ranking_limit or normalized_top_n)))
    normalized_country_limit = max(0, int(country_limit or 0))
    normalized_trend_window = max(1, int(trend_window_months or 16))
    cache_key = _build_hero_product_deck_cache_key(
        countries=countries,
        price_country=price_country,
        tracking_country=tracking_country,
        target_period=target_period,
        time_range=time_range,
        sales_mode=sales_mode,
        segment=segment,
        fuel_types=normalized_fuel_types,
        price_source=price_source,
        top_n=normalized_top_n,
        ranking_limit=normalized_ranking_limit,
        country_limit=normalized_country_limit,
        trend_window_months=normalized_trend_window,
        country_rank_scope=country_rank_scope,
        top_models=top_models,
        hero_models=hero_models,
        dataset_token=dataset_token,
        override_token=override_token,
    )
    now = time.monotonic()
    local_cached = _hero_product_deck_cache.get(cache_key)
    if local_cached is not None:
        cached_at, cached_dataset_token, cached_result = local_cached
        if cached_dataset_token == dataset_token and (now - cached_at) < _HERO_PRODUCT_DECK_CACHE_TTL_SECONDS:
            logger.info("HeroProduct [%s] local-cache HIT", target_period or "latest")
            return cached_result

    redis_client = get_redis_client()
    acquired_cache_lock_key: str | None = None
    if redis_client is not None:
        try:
            cached = get_cached_deck(redis_client, cache_key)
            if cached is not None:
                logger.info("HeroProduct [%s] redis HIT", target_period or "latest")
                with _hero_product_deck_cache_lock:
                    _hero_product_deck_cache[cache_key] = (now, dataset_token, cached)
                return cached
            if acquire_compute_lock(redis_client, cache_key):
                acquired_cache_lock_key = cache_key
            else:
                waited = wait_for_cache(redis_client, cache_key)
                if waited is not None:
                    logger.info("HeroProduct [%s] redis waiter GOT cache from peer", target_period or "latest")
                    with _hero_product_deck_cache_lock:
                        _hero_product_deck_cache[cache_key] = (now, dataset_token, waited)
                    return waited
        except Exception as exc:  # noqa: BLE001 - cache failure should not block deck generation
            logger.warning("HeroProduct cache lookup failed: %s", exc)

    try:
        result = _query_hero_product_deck_impl(
            countries=countries,
            price_country=price_country,
            tracking_country=tracking_country,
            target_period=target_period,
            time_range=time_range,
            sales_mode=sales_mode,
            segment=segment,
            fuel_type=fuel_type,
            fuel_types=normalized_fuel_types,
            price_source=price_source,
            top_n=normalized_top_n,
            ranking_limit=normalized_ranking_limit,
            country_limit=normalized_country_limit,
            trend_window_months=normalized_trend_window,
            country_rank_scope=country_rank_scope,
            top_models=top_models,
            hero_models=hero_models,
        )
    except Exception:
        if redis_client is not None and acquired_cache_lock_key is not None:
            release_compute_lock(redis_client, acquired_cache_lock_key)
        raise

    with _hero_product_deck_cache_lock:
        _hero_product_deck_cache[cache_key] = (now, dataset_token, result)
        if len(_hero_product_deck_cache) > 32:
            oldest_key = min(_hero_product_deck_cache, key=lambda key: _hero_product_deck_cache[key][0])
            _hero_product_deck_cache.pop(oldest_key, None)

    if redis_client is not None:
        try:
            set_cached_deck(redis_client, cache_key, result)
        except Exception as exc:  # noqa: BLE001 - cache write should not fail the API
            logger.warning("HeroProduct cache write failed: %s", exc)
        finally:
            if acquired_cache_lock_key is not None:
                release_compute_lock(redis_client, acquired_cache_lock_key)
    return result


def _query_hero_product_deck_impl(
    *,
    countries: list[str],
    price_country: str | None,
    tracking_country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    sales_mode: str,
    segment: str,
    fuel_type: str,
    fuel_types: list[str] | None = None,
    price_source: str,
    top_n: int,
    ranking_limit: int,
    country_limit: int,
    trend_window_months: int,
    country_rank_scope: str,
    top_models: list[str],
    hero_models: list[str],
) -> dict[str, Any]:
    selected_price_source = price_source if price_source in PRICE_SOURCES else "msrp"
    selected_fuel_types = _normalize_fuel_type_list(fuel_type, fuel_types)
    source = _build_source_frame(
        countries=countries,
        price_country=price_country,
        target_period=target_period,
        time_range=time_range,
        sales_mode=sales_mode,
        segment=segment,
        fuel_types=selected_fuel_types,
        trend_window_months=trend_window_months,
    )
    frame: pd.DataFrame = source["frame"]
    price_frame: pd.DataFrame = source["priceFrame"]
    selected_tracking_country = _resolve_tracking_country(
        tracking_country,
        source["countryOptions"],
        source["priceCountry"],
    )
    brands = set(frame["__brand"].dropna().astype(str).tolist()) if not frame.empty else set()
    current_price_map = _load_current_price_map(source["priceCountry"], brands)
    overrides = _load_price_overrides(source["priceCountry"], source["resolvedPeriod"])
    spec_overrides = _load_spec_overrides(source["priceCountry"], source["resolvedPeriod"])
    model_rows = _aggregate_models(
        frame,
        price_frame,
        source["specColumns"],
        current_price_map,
        overrides,
        spec_overrides,
        selected_price_source,
    )
    ranking_rows = model_rows[:ranking_limit]
    top_rows = _resolve_requested_models(
        model_rows,
        top_models,
        ranking_rows[:top_n],
        top_n,
    )
    hero_rows = _resolve_requested_models(
        model_rows,
        hero_models,
        _default_hero_rows(model_rows, top_n),
        top_n,
    )
    benchmark_keys = {_source_pair(row) for row in [*top_rows[:6], *hero_rows]}
    benchmark_rows = [
        row
        for row in model_rows
        if _source_pair(row) in benchmark_keys
    ]
    top_distribution = _build_country_distribution(frame, top_rows, country_limit)
    hero_distribution = _build_country_distribution(frame, hero_rows, country_limit)
    if country_rank_scope == "selected":
        rank_countries = [selected_tracking_country]
    else:
        rank_countries = _country_rank_trace_options(
            [option["label"] for option in source["countryOptions"]],
            source["countryOptions"],
            selected_tracking_country,
        )
    ranking_rows_for_traces = []
    seen_trace_pairs: set[tuple[str, str]] = set()
    for row in [*top_rows, *hero_rows]:
        pair = _source_pair(row)
        if pair in seen_trace_pairs:
            continue
        ranking_rows_for_traces.append(row)
        seen_trace_pairs.add(pair)
    combined_country_rankings = _build_country_ranking_trace_map(
        frame,
        ranking_rows_for_traces,
        source["trendPeriods"],
        rank_countries,
        ranking_limit,
    )
    top_country_rankings = _filter_country_ranking_trace_map(combined_country_rankings, top_rows)
    hero_country_rankings = _filter_country_ranking_trace_map(combined_country_rankings, hero_rows)
    selected_tracking_key = selected_tracking_country["label"] or selected_tracking_country["value"]
    top_selected_country_ranking = top_country_rankings.get(selected_tracking_key)
    if top_selected_country_ranking is None:
        top_selected_country_ranking = _build_country_ranking_trace(
            frame,
            top_rows,
            source["trendPeriods"],
            selected_tracking_country,
            ranking_limit,
        )
    hero_selected_country_ranking = hero_country_rankings.get(selected_tracking_key)
    if hero_selected_country_ranking is None:
        hero_selected_country_ranking = _build_country_ranking_trace(
            frame,
            hero_rows,
            source["trendPeriods"],
            selected_tracking_country,
            ranking_limit,
        )

    total_sales = sum(float(row["sales"]) for row in model_rows)
    year_text, month_text = source["resolvedPeriod"].split("-", 1)
    month_number = int(month_text)
    page_title = (
        f"{source['selectedSegment']} {source['selectedFuelType']} Hero Product 分析"
    )
    period_label = (
        f"{int(year_text)}年1-{month_number}月"
        if sales_mode == "ytd" and not source["customPeriods"]
        else source["salesModeLabel"]
    )
    market_scope_label = (
        "全部市场"
        if len(source["selectedCountries"]) == len(source["countryOptions"])
        else " / ".join(item["label"] for item in source["selectedCountries"][:4])
    )
    fuel_label = source["selectedFuelType"]

    return {
        "metadata": {
            "protocolVersion": "hero-product/v1",
            "requestedPeriod": target_period,
            "resolvedPeriod": source["resolvedPeriod"],
            "latestPeriod": source["latestPeriod"],
            "selectedCountries": source["selectedCountries"],
            "selectedPriceCountry": source["priceCountry"],
            "selectedTrackingCountry": selected_tracking_country,
            "selectedSegment": source["selectedSegment"],
            "selectedFuelTypes": source["selectedFuelTypes"],
            "selectedFuelType": source["selectedFuelType"],
            "selectedSalesMode": sales_mode,
            "selectedPriceSource": selected_price_source,
            "selectedCountryLimit": max(0, int(country_limit or 0)),
            "selectedTimeRange": {
                "start": source["customPeriods"][0],
                "end": source["customPeriods"][-1],
            } if source["customPeriods"] else None,
            "customRangeActive": bool(source["customPeriods"]),
            "availableCountries": source["countryOptions"],
            "availablePeriods": [
                {"value": period, "label": _short_period_label(period)}
                for period in source["availablePeriods"]
            ],
            "availablePriceSources": [
                {"value": "msrp", "label": "MSRP 抓取价"},
                {"value": "jato", "label": "JATO 价格"},
            ],
            "availableFuelTypes": source["availableFuelTypes"],
            "labels": {
                "pageTitle": page_title,
                "periodLabel": period_label,
                "currentMonthShort": _short_period_label(source["resolvedPeriod"]),
                "salesModeLabel": source["salesModeLabel"],
                "marketScopeLabel": market_scope_label,
            },
        },
        "summary": {
            "totalSales": total_sales,
            "modelCount": len(model_rows),
            "topModel": top_rows[0]["model"] if top_rows else "-",
            "heroModelCount": len(hero_rows),
        },
        "modelOptions": [
            {
                "brand": row["brand"],
                "model": row["model"],
                "sourceBrand": _source_brand(row),
                "sourceModel": _source_model(row),
                "label": row["label"],
                "sales": row["sales"],
            }
            for row in model_rows
        ],
        "pages": {
            "benchmark": {
                "title": f"{fuel_label} 动总产品对标",
                "ranking": ranking_rows,
                "productRows": benchmark_rows,
            },
            "benchmarkWithChannel": {
                "title": f"{fuel_label} 动总渠道结构对标",
                "ranking": ranking_rows,
                "productRows": benchmark_rows,
            },
            "topTrend": {
                "title": "Top 车型销量趋势",
                "models": top_rows,
                "series": _build_trend_series(frame, top_rows, source["trendPeriods"]),
                "countryRanking": top_selected_country_ranking,
                "countryRankings": top_country_rankings,
                "priceRows": _build_price_panel_rows(top_rows[:5]),
            },
            "topDistribution": {
                "title": "Top 车型市场分布",
                "models": top_rows,
                "distribution": top_distribution,
            },
            "heroTrend": {
                "title": "中国车型销量趋势",
                "models": hero_rows,
                "series": _build_trend_series(frame, hero_rows, source["trendPeriods"]),
                "countryRanking": hero_selected_country_ranking,
                "countryRankings": hero_country_rankings,
                "priceRows": _build_price_panel_rows(hero_rows[:6]),
            },
            "heroDistribution": {
                "title": "中国车型市场分布",
                "models": hero_rows,
                "distribution": hero_distribution,
            },
        },
    }


def _override_to_dict(row: HeroProductPriceOverride) -> dict[str, Any]:
    return {
        "overrideId": str(row.override_id),
        "country": row.country,
        "pricePeriod": row.price_period,
        "priceSource": row.price_source,
        "brand": row.brand,
        "model": row.model,
        "trim": row.trim,
        "powertrain": row.powertrain,
        "priceValue": float(row.price_value),
        "currency": row.currency,
        "updatedBy": row.updated_by,
        "note": row.note,
        "createdAt": row.created_at_utc.isoformat() if row.created_at_utc else None,
        "updatedAt": row.updated_at_utc.isoformat() if row.updated_at_utc else None,
    }


def upsert_hero_product_price_override(
    session: Session,
    payload: dict[str, Any],
    *,
    updated_by: str,
) -> dict[str, Any]:
    country = _coerce_text(payload.get("country"))
    price_source = _coerce_text(payload.get("price_source")).lower()
    brand = _coerce_text(payload.get("brand"))
    model = _coerce_text(payload.get("model"))
    if not country or price_source not in PRICE_SOURCES or not brand or not model:
        raise ValueError("country, price_source, brand and model are required")
    price_period = _coerce_text(payload.get("price_period"))
    trim = _coerce_text(payload.get("trim"))
    powertrain = _coerce_text(payload.get("powertrain"))

    stmt = select(HeroProductPriceOverride).where(
        HeroProductPriceOverride.country == country,
        HeroProductPriceOverride.price_period == price_period,
        HeroProductPriceOverride.price_source == price_source,
        HeroProductPriceOverride.brand == brand,
        HeroProductPriceOverride.model == model,
        HeroProductPriceOverride.trim == trim,
        HeroProductPriceOverride.powertrain == powertrain,
    )
    row = session.execute(stmt).scalar_one_or_none()
    price_value = payload.get("price_value")
    if price_value is None:
        if row is not None:
            session.delete(row)
            session.commit()
            invalidate_hero_product_runtime_cache()
        return {
            "deleted": True,
            "country": country,
            "pricePeriod": price_period,
            "priceSource": price_source,
            "brand": brand,
            "model": model,
        }

    numeric_value = float(price_value)
    if numeric_value < 0:
        raise ValueError("price_value must be greater than or equal to 0")

    if row is None:
        row = HeroProductPriceOverride(
            country=country,
            price_period=price_period,
            price_source=price_source,
            brand=brand,
            model=model,
            trim=trim,
            powertrain=powertrain,
            price_value=numeric_value,
            currency=_coerce_text(payload.get("currency")) or DEFAULT_PRICE_CURRENCY,
            updated_by=updated_by,
            note=payload.get("note"),
        )
        session.add(row)
    else:
        row.price_value = numeric_value
        row.currency = _coerce_text(payload.get("currency")) or row.currency or DEFAULT_PRICE_CURRENCY
        row.updated_by = updated_by
        row.note = payload.get("note")
        row.updated_at_utc = datetime.now(timezone.utc)
    session.commit()
    session.refresh(row)
    invalidate_hero_product_runtime_cache()
    return _override_to_dict(row)


def _spec_override_to_dict(row: HeroProductSpecOverride) -> dict[str, Any]:
    return {
        "overrideId": str(row.override_id),
        "country": row.country,
        "pricePeriod": row.price_period,
        "brand": row.brand,
        "model": row.model,
        "fieldName": row.field_name,
        "fieldValue": row.field_value,
        "updatedBy": row.updated_by,
        "note": row.note,
        "createdAt": row.created_at_utc.isoformat() if row.created_at_utc else None,
        "updatedAt": row.updated_at_utc.isoformat() if row.updated_at_utc else None,
    }


def upsert_hero_product_spec_override(
    session: Session,
    payload: dict[str, Any],
    *,
    updated_by: str,
) -> dict[str, Any]:
    country = _coerce_text(payload.get("country"))
    brand = _coerce_text(payload.get("brand"))
    model = _coerce_text(payload.get("model"))
    field_name = _coerce_text(payload.get("field_name"))
    if not country or not brand or not model or not _is_supported_spec_override_field(field_name):
        raise ValueError("country, brand, model and a supported field_name are required")
    price_period = _coerce_text(payload.get("price_period"))
    field_value = _coerce_text(payload.get("field_value"))

    stmt = select(HeroProductSpecOverride).where(
        HeroProductSpecOverride.country == country,
        HeroProductSpecOverride.price_period == price_period,
        HeroProductSpecOverride.brand == brand,
        HeroProductSpecOverride.model == model,
        HeroProductSpecOverride.field_name == field_name,
    )
    row = session.execute(stmt).scalar_one_or_none()
    if not field_value:
        if row is not None:
            session.delete(row)
            session.commit()
            invalidate_hero_product_runtime_cache()
        return {
            "deleted": True,
            "country": country,
            "pricePeriod": price_period,
            "brand": brand,
            "model": model,
            "fieldName": field_name,
        }

    if row is None:
        row = HeroProductSpecOverride(
            country=country,
            price_period=price_period,
            brand=brand,
            model=model,
            field_name=field_name,
            field_value=field_value,
            updated_by=updated_by,
            note=_coerce_text(payload.get("note")) or None,
        )
        session.add(row)
    else:
        row.field_value = field_value
        row.updated_by = updated_by
        row.note = _coerce_text(payload.get("note")) or None
        row.updated_at_utc = datetime.now(timezone.utc)
    session.commit()
    session.refresh(row)
    invalidate_hero_product_runtime_cache()
    return _spec_override_to_dict(row)
