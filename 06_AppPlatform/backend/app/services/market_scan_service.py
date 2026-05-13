from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import pandas as pd

try:
    import duckdb
except ImportError:  # pragma: no cover - optional runtime dependency
    duckdb = None

from app.db.session import get_engine
from app.infra import parquet_repository as repo

_DECK_CACHE_TTL_SECONDS = 300
_deck_cache: dict[str, tuple[float, str, dict[str, Any]]] = {}
_deck_cache_lock = threading.Lock()

MONTH_NAME_TO_NUMBER = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
NUMBER_TO_MONTH_NAME = {value: key for key, value in MONTH_NAME_TO_NUMBER.items()}
DEFAULT_FUEL_TYPES = ("ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG")
DRILLDOWN_PANEL_FUELS = ("BEV", "PHEV", "HEV", "MHEV", "ICE")
POSITIONING_FUEL_ORDER = ("BEV", "HEV", "PHEV", "MHEV", "ICE")
VERSION_COMPARISON_MODEL_LIMIT = 10
PRIMARY_ORIGINS = ("欧系", "日系", "韩系", "美系", "中系")
ORIGIN_BRAND_TREND_LIMIT = 4
MONTHLY_BRAND_MODEL_STACK_LIMIT = 10
SEGMENT_MATRIX_ORDER = (
    "SUV-A00",
    "SUV-A0",
    "SUV-A",
    "≥SUV-B",
    "SD-A00",
    "SD-A0",
    "SD-A",
    "SD-B",
    "SD-C",
)
SUV_SEGMENT_SHARE_ORDER = (
    "SUV-A00",
    "SUV-A0",
    "SUV-A",
    "≥SUV-B",
)
SEDAN_SEGMENT_SHARE_ORDER = (
    "SD-A00",
    "SD-A0",
    "SD-A",
    "SD-B",
    "SD-C",
)
DEFAULT_DRILLDOWN_SEGMENT = "SUV A0"
FALLBACK_DRILLDOWN_FUELS = ("BEV", "PHEV")
MIN_MARKET_SCAN_RANKING_LIMIT = 10
POSITIONING_BUBBLE_LIMIT = 60
POSITIONING_SALES_MODES = ("month", "ytd", "rolling12")
MSRP_CANDIDATES = (
    "MSRP规整",
    "MSRP including delivery charge",
    "MSRP",
    "MSRP区间",
)
LENGTH_CANDIDATES = (
    "length (mm)",
    "车长(mm)",
    "车长",
    "length",
)
VERSION_CANDIDATES = (
    "Version name",
    "version name",
    "Version Name",
    "Version",
    "版本",
    "版型",
)
TRIM_CANDIDATES = (
    "Trim level",
    "Trim Level",
    "trim level",
    "Trim",
    "trim",
    "配置",
)
OVERLAY_KEY_PATTERN = re.compile(r"[^0-9a-z\u4e00-\u9fff]+", re.IGNORECASE)


@dataclass(frozen=True)
class ColumnMap:
    country_value: str
    country_label: str | None
    make: str
    model: str
    version: str | None
    trim: str | None
    length: str | None
    msrp: str | None
    origin: str | None
    segment: str
    powertrain: str
    body_type: str | None
    drive_type: str | None
    registration_type: str | None
    month_columns: tuple[str, ...]


def _resolve_existing_column(candidates: list[str], columns: list[str]) -> str | None:
    normalized = {str(column).strip().lower(): str(column).strip() for column in columns}
    for candidate in candidates:
        hit = normalized.get(str(candidate).strip().lower())
        if hit:
            return hit
    return None


def _list_month_columns(columns: list[str]) -> list[str]:
    def sort_key(column: str) -> tuple[int, int]:
        parts = column.split(" ", 1)
        if len(parts) != 2:
            return (0, 0)
        year_text, month_name = parts
        return (int(year_text), MONTH_NAME_TO_NUMBER.get(month_name, 0))

    month_columns = [
        column
        for column in columns
        if len(column) >= 8 and column[:4].isdigit() and " " in column
    ]
    return sorted(month_columns, key=sort_key)


@lru_cache(maxsize=8)
def _resolve_columns(dataset_token: str) -> ColumnMap:
    columns = repo.list_columns()
    country_value = _resolve_existing_column(["国家", "Country", "country"], columns)
    make = _resolve_existing_column(["Make", "品牌 (英)", "品牌", "make"], columns)
    model = _resolve_existing_column(["车型规整", "Model", "model"], columns)
    segment = _resolve_existing_column(["细分市场（按车长）", "JATO global segment", "细分市场"], columns)
    powertrain = _resolve_existing_column(["动总规整", "Powertrain type", "Fuel type", "powertrain"], columns)
    if country_value is None or make is None or model is None or segment is None or powertrain is None:
        missing = {
            "country": country_value,
            "make": make,
            "model": model,
            "segment": segment,
            "powertrain": powertrain,
        }
        raise RuntimeError(f"Market scan columns are incomplete: {missing}")
    return ColumnMap(
        country_value=country_value,
        country_label=_resolve_existing_column(["Countries", "Country"], columns),
        make=make,
        model=model,
        version=_resolve_existing_column(list(VERSION_CANDIDATES), columns),
        trim=_resolve_existing_column(list(TRIM_CANDIDATES), columns),
        length=_resolve_existing_column(list(LENGTH_CANDIDATES), columns),
        msrp=_resolve_existing_column(list(MSRP_CANDIDATES), columns),
        origin=_resolve_existing_column(["车系", "Origin", "Series"], columns),
        segment=segment,
        powertrain=powertrain,
        drive_type=_resolve_existing_column(["Driven wheels", "Drive type", "Drive", "驱动形式"], columns),
        body_type=_resolve_existing_column(["Body type", "Body Type", "body type", "车身形式"], columns),
        registration_type=_resolve_existing_column(["Registration type", "Registration Type", "registration type"], columns),
        month_columns=tuple(_list_month_columns(columns)),
    )


def _get_columns() -> ColumnMap:
    return _resolve_columns(repo.current_dataset_token())


def _month_column_to_period(column: str) -> str:
    year_text, month_name = column.split(" ", 1)
    month_number = MONTH_NAME_TO_NUMBER[month_name]
    return f"{int(year_text):04d}-{month_number:02d}"


def _period_to_month_column(period: str) -> str:
    year_text, month_text = period.split("-", 1)
    return f"{int(year_text):04d} {NUMBER_TO_MONTH_NAME[int(month_text)]}"


def _period_to_timestamp(period: str) -> pd.Timestamp:
    return pd.Period(period, freq="M").to_timestamp()


def _shift_period(period: str, months: int) -> str:
    shifted = pd.Period(period, freq="M") + months
    return f"{shifted.year:04d}-{shifted.month:02d}"


def _short_period_label(period: str) -> str:
    year_text, month_text = period.split("-", 1)
    return f"{year_text[2:4]}.{int(month_text):02d}"


def _normalize_drive_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "OTHER"
    if any(token in text for token in ("awd", "4wd", "4x4", "all wheel", "four wheel", "quattro", "4-matic", "4matic", "xdrive", "all", "four")):
        return "4WD"
    if any(token in text for token in ("fwd", "rwd", "2wd", "front wheel", "rear wheel", "two wheel", "sdrive", "front", "rear")):
        return "2WD"
    return "OTHER"


def _normalize_registration_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text or text in {"?", "unknown", "n/a", "na"}:
        return "Other"
    if any(token in text for token in ("business", "fleet", "company", "corporate", "lease", "leasing", "rental")):
        return "Business"
    if any(token in text for token in ("private", "retail", "personal", "consumer")):
        return "Private"
    return "Other"


def _registration_mix_payload(group: pd.DataFrame, current_columns: list[str]) -> dict[str, float]:
    if "__registration_type" not in group.columns:
        return {"Business": 0.0, "Private": 0.0, "Other": 0.0}
    return {
        registration_type: float(_series_sum(group[group["__registration_type"] == registration_type], current_columns).sum())
        for registration_type in ("Business", "Private", "Other")
    }


def _build_channel_mix_items(
    frame: pd.DataFrame,
    current_columns: list[str],
    *,
    group_column: str = "__origin",
    limit: int = 6,
) -> list[dict[str, Any]]:
    if frame.empty or not current_columns or "__registration_type" not in frame.columns:
        return []
    working = frame.copy()
    if group_column in working.columns:
        working["__channel_mix_group"] = working[group_column].fillna("其他").astype(str)
    else:
        working["__channel_mix_group"] = "整体市场"
    working["__channel_mix_volume"] = _series_sum(working, current_columns)
    grouped_volume = (
        working.groupby("__channel_mix_group", dropna=False)["__channel_mix_volume"]
        .sum()
        .sort_values(ascending=False)
    )
    grouped_volume = grouped_volume[grouped_volume > 0].head(max(1, int(limit)))

    items: list[dict[str, Any]] = []
    for label, volume in grouped_volume.items():
        group = working[working["__channel_mix_group"] == label]
        channel_mix = _registration_mix_payload(group, current_columns)
        denominator = float(volume) if float(volume) > 0 else sum(channel_mix.values())
        items.append(
            {
                "label": str(label),
                "volume": float(volume),
                "channelMix": channel_mix,
                "channelSharePct": {
                    channel: _safe_share(value, denominator)
                    for channel, value in channel_mix.items()
                },
            }
        )
    return items


CHANNEL_MIX_OPTIONS = [
    {"value": "overall", "label": "整体市场"},
    {"value": "origin", "label": "按车系"},
]


def _build_channel_mix_window(
    frame: pd.DataFrame,
    current_columns: list[str],
    *,
    title: str,
) -> dict[str, Any]:
    overall_items = _build_channel_mix_items(
        frame,
        current_columns,
        group_column="__overall_market",
        limit=1,
    )
    return {
        "title": title,
        "defaultView": "origin",
        "items": overall_items,
        "views": {
            "overall": {
                "title": f"{title} · Overall",
                "items": overall_items,
            },
            "origin": {
                "title": f"{title} · Origin",
                "items": _build_channel_mix_items(
                    frame,
                    current_columns,
                    group_column="__origin",
                    limit=6,
                ),
            },
        },
    }


def _resolve_period(requested_period: str | None, available_periods: list[str]) -> str:
    if not available_periods:
        raise RuntimeError("No month columns available for market scan")
    if not requested_period:
        return available_periods[-1]

    normalized = requested_period.strip().replace("/", "-").replace(".", "-")
    if len(normalized) == 7 and normalized[4] == "-":
        target = normalized
    else:
        return available_periods[-1]

    if target in available_periods:
        return target

    target_ts = _period_to_timestamp(target)
    fallback_candidates = [period for period in available_periods if _period_to_timestamp(period) <= target_ts]
    if fallback_candidates:
        return fallback_candidates[-1]
    return available_periods[-1]


def _normalize_country_lookup(country: str | None, country_options: list[dict[str, str]]) -> dict[str, str]:
    if not country_options:
        raise RuntimeError("No country options available for market scan")

    default_match = next(
        (
            option
            for option in country_options
            if option["label"].strip().lower() == "hungary"
            or option["value"].strip() == "匈牙利"
        ),
        country_options[0],
    )
    if not country:
        return default_match

    normalized = country.strip().lower()
    for option in country_options:
        if option["value"].strip().lower() == normalized:
            return option
        if option["label"].strip().lower() == normalized:
            return option
    return default_match


def _coerce_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_overlay_key(value: object) -> str:
    text = _coerce_text(value).lower()
    if not text:
        return ""
    return OVERLAY_KEY_PATTERN.sub("", text)


def _country_overlay_candidates(selected_country: dict[str, str]) -> list[str]:
    raw_candidates = [selected_country.get("value"), selected_country.get("label")]
    candidates: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_candidates:
        text = _coerce_text(raw_value)
        if not text:
            continue
        parts = [text]
        if "/" in text:
            parts.extend(part.strip() for part in text.split("/") if part.strip())
        for part in parts:
            normalized = part.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(part)
    return candidates


def _sql_quote_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_quote_literal_list(values: list[str]) -> str:
    return ", ".join(
        _sql_quote_literal(value)
        for value in values
        if _coerce_text(value)
    )


def _normalize_duckdb_postgres_url(raw_url: str) -> str:
    return (
        str(raw_url)
        .replace("postgresql+psycopg2://", "postgresql://")
        .replace("postgresql+psycopg://", "postgresql://")
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("postgresql+aiopg://", "postgresql://")
    )


def _load_duckdb_postgres_extension(connection: Any) -> None:
    attempts = (
        ("LOAD postgres",),
        ("LOAD postgres_scanner",),
        ("INSTALL postgres", "LOAD postgres"),
        ("INSTALL postgres_scanner", "LOAD postgres_scanner"),
    )
    last_error: Exception | None = None
    for statements in attempts:
        try:
            for statement in statements:
                connection.execute(statement)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError("DuckDB postgres extension is unavailable") from last_error


def _build_positioning_current_price_candidates(price_frame: pd.DataFrame) -> pd.DataFrame:
    if price_frame.empty:
        return pd.DataFrame(
            columns=[
                "brand_norm",
                "model_norm",
                "trim_norm",
                "powertrain_norm",
                "current_msrp_value",
                "currency",
                "source_url",
                "match_confidence",
                "updated_at_utc",
                "source_tier",
                "source_code",
                "source_type",
                "match_variant",
            ]
        )

    candidate_rows: list[dict[str, Any]] = []
    for row in price_frame.to_dict("records"):
        brand_norm = _normalize_overlay_key(row.get("brand"))
        if not brand_norm:
            continue
        official_trim_parts = [
            _coerce_text(row.get("official_trim")),
            _coerce_text(row.get("official_edition")),
        ]
        official_trim = " ".join(part for part in official_trim_parts if part).strip()
        for model_field, trim_field, powertrain_field, match_variant in (
            ("jato_model", "jato_trim", "jato_powertrain", "jato"),
            ("official_model", "__official_trim", "official_powertrain", "official"),
        ):
            normalized_trim_source = official_trim if trim_field == "__official_trim" else row.get(trim_field)
            model_norm = _normalize_overlay_key(row.get(model_field))
            if not model_norm:
                continue
            normalized_powertrain = _normalize_powertrain(row.get(powertrain_field))
            candidate_rows.append(
                {
                    "brand_norm": brand_norm,
                    "model_norm": model_norm,
                    "trim_norm": _normalize_overlay_key(normalized_trim_source),
                    "powertrain_norm": ""
                    if normalized_powertrain == "OTHER"
                    else _normalize_overlay_key(normalized_powertrain),
                    "current_msrp_value": row.get("current_msrp_value"),
                    "currency": _coerce_text(row.get("currency")),
                    "source_url": _coerce_text(row.get("source_url")),
                    "match_confidence": row.get("match_confidence"),
                    "updated_at_utc": row.get("updated_at_utc"),
                    "source_tier": row.get("source_tier"),
                    "source_code": _coerce_text(row.get("source_code")),
                    "source_type": _coerce_text(row.get("source_type")),
                    "match_variant": match_variant,
                }
            )

    if not candidate_rows:
        return pd.DataFrame(
            columns=[
                "brand_norm",
                "model_norm",
                "trim_norm",
                "powertrain_norm",
                "current_msrp_value",
                "currency",
                "source_url",
                "match_confidence",
                "updated_at_utc",
                "source_tier",
                "source_code",
                "source_type",
                "match_variant",
            ]
        )

    candidates = pd.DataFrame(candidate_rows)
    candidates["current_msrp_value"] = pd.to_numeric(
        candidates["current_msrp_value"],
        errors="coerce",
    ).fillna(0.0)
    candidates["match_confidence"] = pd.to_numeric(
        candidates["match_confidence"],
        errors="coerce",
    ).fillna(0.0)
    candidates["updated_at_utc"] = pd.to_datetime(
        candidates["updated_at_utc"],
        errors="coerce",
    )
    candidates["source_tier"] = pd.to_numeric(
        candidates["source_tier"],
        errors="coerce",
    ).fillna(3).astype(int)
    candidates = candidates[candidates["current_msrp_value"] > 0].copy()
    if candidates.empty:
        return candidates
    return candidates.drop_duplicates(
        subset=[
            "brand_norm",
            "model_norm",
            "trim_norm",
            "powertrain_norm",
            "current_msrp_value",
            "source_url",
            "source_code",
            "match_variant",
        ]
    ).reset_index(drop=True)


def _build_positioning_jato_link_candidates(link_frame: pd.DataFrame) -> pd.DataFrame:
    if link_frame.empty:
        return pd.DataFrame(
            columns=[
                "brand_norm",
                "jato_model_norm",
                "jato_trim_norm",
                "jato_powertrain_norm",
                "official_model_norm",
                "official_trim_norm",
                "official_powertrain_norm",
                "confidence",
                "link_source",
            ]
        )

    rows: list[dict[str, Any]] = []
    for row in link_frame.to_dict("records"):
        brand_norm = _normalize_overlay_key(row.get("brand"))
        jato_model_norm = _normalize_overlay_key(row.get("jato_model"))
        official_model_norm = _normalize_overlay_key(row.get("official_model"))
        if not brand_norm or not jato_model_norm or not official_model_norm:
            continue
        official_trim_parts = [
            _coerce_text(row.get("official_trim")),
            _coerce_text(row.get("official_edition")),
        ]
        jato_powertrain = _normalize_powertrain(row.get("jato_powertrain"))
        official_powertrain = _normalize_powertrain(row.get("official_powertrain"))
        rows.append(
            {
                "brand_norm": brand_norm,
                "jato_model_norm": jato_model_norm,
                "jato_trim_norm": _normalize_overlay_key(row.get("jato_trim")),
                "jato_powertrain_norm": ""
                if jato_powertrain == "OTHER"
                else _normalize_overlay_key(jato_powertrain),
                "official_model_norm": official_model_norm,
                "official_trim_norm": _normalize_overlay_key(
                    " ".join(part for part in official_trim_parts if part)
                ),
                "official_powertrain_norm": ""
                if official_powertrain == "OTHER"
                else _normalize_overlay_key(official_powertrain),
                "confidence": row.get("confidence"),
                "link_source": _coerce_text(row.get("link_source")),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "brand_norm",
                "jato_model_norm",
                "jato_trim_norm",
                "jato_powertrain_norm",
                "official_model_norm",
                "official_trim_norm",
                "official_powertrain_norm",
                "confidence",
                "link_source",
            ]
        )

    candidates = pd.DataFrame(rows)
    candidates["confidence"] = pd.to_numeric(
        candidates["confidence"],
        errors="coerce",
    ).fillna(0.0)
    return candidates.drop_duplicates().reset_index(drop=True)


def _load_positioning_overlay_candidates(
    selected_country: dict[str, str],
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    country_candidates = _country_overlay_candidates(selected_country)
    if not country_candidates:
        return pd.DataFrame(), pd.DataFrame(), {
            "mode": "parquet-only",
            "reason": "country-unresolved",
        }

    if duckdb is None:
        return pd.DataFrame(), pd.DataFrame(), {
            "mode": "parquet-only",
            "reason": "duckdb-unavailable",
        }

    try:
        engine = get_engine()
    except Exception as exc:  # noqa: BLE001
        return pd.DataFrame(), pd.DataFrame(), {
            "mode": "parquet-only",
            "reason": f"database-unavailable: {exc}",
        }

    brand_candidates = sorted(
        {
            _coerce_text(value)
            for value in frame.get("__brand", pd.Series(dtype=str)).tolist()
            if _coerce_text(value)
        }
    )
    country_filter_sql = _sql_quote_literal_list(
        [candidate.lower() for candidate in country_candidates]
    )
    brand_filter_sql = ""
    if brand_candidates:
        brand_filter_sql = (
            f"AND cp.brand IN ({_sql_quote_literal_list(brand_candidates)})"
        )

    connection = None
    try:
        connection = duckdb.connect(database=":memory:")
        _load_duckdb_postgres_extension(connection)
        postgres_url = _normalize_duckdb_postgres_url(
            engine.url.render_as_string(hide_password=False)
        )
        connection.execute(
            f"ATTACH {_sql_quote_literal(postgres_url)} AS msrp_pg (TYPE POSTGRES, READ_ONLY)"
        )
        current_price_frame = connection.execute(
            f"""
            SELECT
                cp.country AS country,
                cp.brand AS brand,
                cp.jato_model AS jato_model,
                cp.jato_trim AS jato_trim,
                cp.jato_powertrain AS jato_powertrain,
                cp.official_model AS official_model,
                cp.official_trim AS official_trim,
                COALESCE(cp.official_edition, '') AS official_edition,
                COALESCE(cp.official_powertrain, '') AS official_powertrain,
                cp.current_msrp_value AS current_msrp_value,
                cp.currency AS currency,
                cp.match_confidence AS match_confidence,
                cp.source_url AS source_url,
                cp.updated_at_utc AS updated_at_utc,
                COALESCE(src.tier, 3) AS source_tier,
                COALESCE(src.source_code, '') AS source_code,
                COALESCE(src.source_type, '') AS source_type
            FROM msrp_pg.msrp.current_prices cp
            LEFT JOIN msrp_pg.msrp.observations obs
              ON obs.observation_id = cp.effective_observation_id
            LEFT JOIN msrp_pg.msrp.sources src
              ON src.source_id = obs.source_id
            WHERE lower(cp.country) IN ({country_filter_sql})
              {brand_filter_sql}
            """
        ).fetchdf()
    except Exception as exc:  # noqa: BLE001
        return pd.DataFrame(), pd.DataFrame(), {
            "mode": "parquet-only",
            "reason": f"duckdb-postgres-attach-failed: {exc}",
        }

    link_query_reason = None
    try:
        link_frame = connection.execute(
            f"""
            SELECT
                country,
                brand,
                jato_model,
                jato_trim,
                jato_powertrain,
                official_model,
                official_trim,
                official_edition,
                official_powertrain,
                confidence,
                link_source
            FROM msrp_pg.msrp.jato_msrp_links
            WHERE is_active = true
              AND lower(country) IN ({country_filter_sql})
              {brand_filter_sql.replace('cp.brand', 'brand')}
            """
        ).fetchdf()
    except Exception as exc:  # noqa: BLE001
        link_frame = pd.DataFrame()
        link_query_reason = f"link-query-failed: {exc}"
    finally:
        if connection is not None:
            connection.close()

    candidates = _build_positioning_current_price_candidates(current_price_frame)
    link_candidates = _build_positioning_jato_link_candidates(link_frame)
    meta = {
        "mode": "duckdb-postgres-attach",
        "candidateRows": int(len(candidates)),
        "linkCandidateRows": int(len(link_candidates)),
        "countryCandidates": country_candidates,
        "brandCandidates": brand_candidates,
    }
    if link_query_reason:
        meta["linkReason"] = link_query_reason
    if candidates.empty:
        meta["reason"] = "no-current-prices"
        return pd.DataFrame(), link_candidates, meta
    return candidates, link_candidates, meta


def _apply_positioning_current_price_overlay(
    frame: pd.DataFrame,
    current_price_candidates: pd.DataFrame,
    jato_link_candidates: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if frame.empty:
        return frame, {"mode": "parquet-only", "reason": "no-positioning-rows"}
    if current_price_candidates.empty:
        return frame, {"mode": "parquet-only", "reason": "no-current-price-candidates"}
    if duckdb is None:
        return frame, {"mode": "parquet-only", "reason": "duckdb-unavailable"}

    working = frame.copy()
    working["__row_id"] = range(len(working))
    if "__trim" in working.columns:
        working["__trim"] = working["__trim"].fillna("").astype(str).str.strip()
    else:
        working["__trim"] = ""
    working["__brand_norm"] = working["__brand"].map(_normalize_overlay_key)
    working["__model_norm"] = working["__model"].map(_normalize_overlay_key)
    working["__trim_norm"] = working["__trim"].map(_normalize_overlay_key)
    working["__powertrain_norm"] = working["__powertrain"].map(_normalize_overlay_key)
    link_candidates = (
        jato_link_candidates.copy()
        if jato_link_candidates is not None
        else pd.DataFrame(
            columns=[
                "brand_norm",
                "jato_model_norm",
                "jato_trim_norm",
                "jato_powertrain_norm",
                "official_model_norm",
                "official_trim_norm",
                "official_powertrain_norm",
                "confidence",
                "link_source",
            ]
        )
    )

    connection = None
    try:
        connection = duckdb.connect(database=":memory:")
        connection.register("parquet_rows", working)
        connection.register("current_price_rows", current_price_candidates)
        connection.register("link_rows", link_candidates)
        overlay = connection.execute(
            """
            WITH link_matched AS (
                SELECT
                    parquet_rows.__row_id AS row_id,
                    current_price_rows.current_msrp_value,
                    current_price_rows.currency,
                    current_price_rows.source_url,
                    current_price_rows.match_confidence,
                    current_price_rows.updated_at_utc,
                    current_price_rows.match_variant,
                    current_price_rows.source_tier,
                    current_price_rows.source_code,
                    current_price_rows.source_type,
                    COALESCE(link_rows.confidence, 0) AS link_confidence,
                    COALESCE(link_rows.link_source, '') AS link_source,
                    'link' AS overlay_strategy,
                    CASE
                        WHEN parquet_rows.__trim_norm <> ''
                             AND current_price_rows.trim_norm <> ''
                             AND parquet_rows.__trim_norm = current_price_rows.trim_norm
                            THEN 0
                        WHEN parquet_rows.__trim_norm <> ''
                             AND current_price_rows.trim_norm <> ''
                             AND (
                                strpos(parquet_rows.__trim_norm, current_price_rows.trim_norm) > 0
                                OR strpos(current_price_rows.trim_norm, parquet_rows.__trim_norm) > 0
                             )
                            THEN 1
                        WHEN parquet_rows.__trim_norm = '' OR current_price_rows.trim_norm = ''
                            THEN 2
                        ELSE 3
                    END AS trim_rank,
                    CASE
                        WHEN parquet_rows.__powertrain_norm <> ''
                             AND parquet_rows.__powertrain_norm = current_price_rows.powertrain_norm
                            THEN 0
                        WHEN parquet_rows.__powertrain_norm = '' OR current_price_rows.powertrain_norm = ''
                            THEN 1
                        ELSE 2
                    END AS powertrain_rank,
                    0 AS match_variant_rank
                FROM parquet_rows
                JOIN link_rows
                  ON parquet_rows.__brand_norm = link_rows.brand_norm
                 AND parquet_rows.__model_norm = link_rows.jato_model_norm
                 AND (
                    parquet_rows.__powertrain_norm = ''
                    OR link_rows.jato_powertrain_norm = ''
                    OR parquet_rows.__powertrain_norm = link_rows.jato_powertrain_norm
                 )
                 AND (
                    parquet_rows.__trim_norm = ''
                    OR link_rows.jato_trim_norm = ''
                    OR parquet_rows.__trim_norm = link_rows.jato_trim_norm
                    OR strpos(parquet_rows.__trim_norm, link_rows.jato_trim_norm) > 0
                    OR strpos(link_rows.jato_trim_norm, parquet_rows.__trim_norm) > 0
                 )
                JOIN current_price_rows
                  ON current_price_rows.match_variant = 'official'
                 AND parquet_rows.__brand_norm = current_price_rows.brand_norm
                 AND current_price_rows.model_norm = link_rows.official_model_norm
                 AND (
                    link_rows.official_powertrain_norm = ''
                    OR current_price_rows.powertrain_norm = ''
                    OR link_rows.official_powertrain_norm = current_price_rows.powertrain_norm
                 )
                 AND (
                    link_rows.official_trim_norm = ''
                    OR current_price_rows.trim_norm = ''
                    OR link_rows.official_trim_norm = current_price_rows.trim_norm
                    OR strpos(link_rows.official_trim_norm, current_price_rows.trim_norm) > 0
                    OR strpos(current_price_rows.trim_norm, link_rows.official_trim_norm) > 0
                 )
            ),
            direct_matched AS (
                SELECT
                    parquet_rows.__row_id AS row_id,
                    current_price_rows.current_msrp_value,
                    current_price_rows.currency,
                    current_price_rows.source_url,
                    current_price_rows.match_confidence,
                    current_price_rows.updated_at_utc,
                    current_price_rows.match_variant,
                    current_price_rows.source_tier,
                    current_price_rows.source_code,
                    current_price_rows.source_type,
                    0.0 AS link_confidence,
                    '' AS link_source,
                    'direct' AS overlay_strategy,
                    CASE
                        WHEN parquet_rows.__trim_norm <> ''
                             AND current_price_rows.trim_norm <> ''
                             AND parquet_rows.__trim_norm = current_price_rows.trim_norm
                            THEN 0
                        WHEN parquet_rows.__trim_norm <> ''
                             AND current_price_rows.trim_norm <> ''
                             AND (
                                strpos(parquet_rows.__trim_norm, current_price_rows.trim_norm) > 0
                                OR strpos(current_price_rows.trim_norm, parquet_rows.__trim_norm) > 0
                             )
                            THEN 1
                        WHEN parquet_rows.__trim_norm = '' OR current_price_rows.trim_norm = ''
                            THEN 2
                        ELSE 3
                    END AS trim_rank,
                    CASE
                        WHEN parquet_rows.__powertrain_norm <> ''
                             AND parquet_rows.__powertrain_norm = current_price_rows.powertrain_norm
                            THEN 0
                        WHEN parquet_rows.__powertrain_norm = '' OR current_price_rows.powertrain_norm = ''
                            THEN 1
                        ELSE 2
                    END AS powertrain_rank,
                    CASE
                        WHEN current_price_rows.match_variant = 'jato' THEN 0
                        ELSE 1
                    END AS match_variant_rank
                FROM parquet_rows
                JOIN current_price_rows
                  ON parquet_rows.__brand_norm = current_price_rows.brand_norm
                 AND parquet_rows.__model_norm = current_price_rows.model_norm
                 AND (
                    parquet_rows.__powertrain_norm = ''
                    OR current_price_rows.powertrain_norm = ''
                    OR parquet_rows.__powertrain_norm = current_price_rows.powertrain_norm
                 )
                 AND (
                    parquet_rows.__trim_norm = ''
                    OR current_price_rows.trim_norm = ''
                    OR parquet_rows.__trim_norm = current_price_rows.trim_norm
                    OR strpos(parquet_rows.__trim_norm, current_price_rows.trim_norm) > 0
                    OR strpos(current_price_rows.trim_norm, parquet_rows.__trim_norm) > 0
                 )
            ),
            matched AS (
                SELECT * FROM link_matched
                UNION ALL
                SELECT * FROM direct_matched
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY row_id
                        ORDER BY
                            CASE overlay_strategy
                                WHEN 'link' THEN 0
                                ELSE 1
                            END,
                            trim_rank,
                            powertrain_rank,
                            match_variant_rank,
                            COALESCE(source_tier, 3) ASC,
                            COALESCE(link_confidence, 0) DESC,
                            COALESCE(match_confidence, 0) DESC,
                            updated_at_utc DESC,
                            current_msrp_value DESC
                    ) AS match_rank
                FROM matched
            )
            SELECT
                row_id,
                current_msrp_value,
                currency,
                source_url,
                match_confidence,
                updated_at_utc,
                match_variant
                ,source_tier,
                source_code,
                source_type,
                link_confidence,
                link_source,
                overlay_strategy
            FROM ranked
            WHERE match_rank = 1
            """
        ).fetchdf()
    except Exception as exc:  # noqa: BLE001
        return frame, {"mode": "parquet-only", "reason": f"duckdb-overlay-failed: {exc}"}
    finally:
        if connection is not None:
            connection.close()

    if overlay.empty:
        return frame, {"mode": "parquet-only", "reason": "no-overlay-matches"}

    merged = working.merge(
        overlay,
        how="left",
        left_on="__row_id",
        right_on="row_id",
    )
    merged["__msrp_parquet"] = merged["__msrp"]
    overlay_mask = pd.to_numeric(
        merged["current_msrp_value"],
        errors="coerce",
    ).fillna(0.0) > 0
    merged.loc[overlay_mask, "__msrp"] = pd.to_numeric(
        merged.loc[overlay_mask, "current_msrp_value"],
        errors="coerce",
    ).fillna(0.0)
    merged["__msrp_source"] = "parquet"
    merged.loc[overlay_mask, "__msrp_source"] = "current_prices"
    merged["__msrp_source_url"] = merged["source_url"].fillna("")
    merged["__msrp_match_variant"] = merged["match_variant"].fillna("")
    merged["__msrp_overlay_strategy"] = merged["overlay_strategy"].fillna("")
    merged["__msrp_match_confidence"] = pd.to_numeric(
        merged["match_confidence"],
        errors="coerce",
    ).fillna(0.0)
    merged["__msrp_source_tier"] = pd.to_numeric(
        merged["source_tier"],
        errors="coerce",
    ).fillna(0).astype(int)
    merged["__msrp_source_code"] = merged["source_code"].fillna("")
    merged["__msrp_source_type"] = merged["source_type"].fillna("")
    merged["__msrp_link_confidence"] = pd.to_numeric(
        merged["link_confidence"],
        errors="coerce",
    ).fillna(0.0)
    merged["__msrp_link_source"] = merged["link_source"].fillna("")
    merged = merged.drop(
        columns=[
            "row_id",
            "current_msrp_value",
            "currency",
            "source_url",
            "match_confidence",
            "updated_at_utc",
            "match_variant",
            "source_tier",
            "source_code",
            "source_type",
            "link_confidence",
            "link_source",
            "overlay_strategy",
        ],
        errors="ignore",
    )
    link_matches = 0
    direct_matches = 0
    if not overlay.empty and "overlay_strategy" in overlay.columns:
        strategies = overlay["overlay_strategy"].fillna("")
        link_matches = int((strategies == "link").sum())
        direct_matches = int((strategies == "direct").sum())
    return merged, {
        "mode": "duckdb-overlay",
        "matchedRows": int(overlay_mask.sum()),
        "matchedModels": int(merged.loc[overlay_mask, "__model"].nunique()),
        "linkMatches": link_matches,
        "directMatches": direct_matches,
    }


def _normalize_powertrain(value: object) -> str:
    text = _coerce_text(value).upper()
    if not text or text == "?":
        return "OTHER"
    if text == "COMBUSTION":
        return "ICE"
    if text == "EREV":
        return "REEV"
    if text == "FCEV":
        return "FCV"
    return text


def _normalize_origin(value: object) -> str:
    text = _coerce_text(value)
    if not text or text == "?":
        return "其他"
    if text == "中系2":
        return "中系"
    if text in PRIMARY_ORIGINS:
        return text
    return text or "其他"


def _normalize_segment_lookup(value: str) -> str:
    return value.replace("-", "").replace(" ", "").upper()


def _resolve_segment_value(requested_segment: str | None, available_segments: list[str], fallback: str) -> str:
    if not available_segments:
        return fallback
    normalized_lookup = {_normalize_segment_lookup(segment): segment for segment in available_segments}
    if requested_segment:
        hit = normalized_lookup.get(_normalize_segment_lookup(requested_segment))
        if hit:
            return hit
    fallback_hit = normalized_lookup.get(_normalize_segment_lookup(fallback))
    if fallback_hit:
        return fallback_hit
    return available_segments[0]


def _segment_display_label(segment: str) -> str:
    return segment.replace(" ", "-").replace("及以上", "+")


def _segment_matrix_bucket(segment: object) -> str | None:
    text = _coerce_text(segment)
    if not text:
        return None
    if text.startswith("SUV A00"):
        return "SUV-A00"
    if text.startswith("SUV A0"):
        return "SUV-A0"
    if text == "SUV A":
        return "SUV-A"
    if text.startswith("SUV B") or text.startswith("SUV C") or text.startswith("SUV D"):
        return "≥SUV-B"
    if text.startswith("Car A00"):
        return "SD-A00"
    if text.startswith("Car A0"):
        return "SD-A0"
    if text == "Car A":
        return "SD-A"
    if text.startswith("Car B"):
        return "SD-B"
    if text.startswith("Car C") or text.startswith("Car D") or text.startswith("Car E"):
        return "SD-C"
    return None


def _delta_payload(current_value: float, base_value: float) -> dict[str, Any]:
    current = float(current_value or 0.0)
    base = float(base_value or 0.0)
    if base == 0:
        if current == 0:
            return {"value": None, "display": "-", "tone": "neutral"}
        return {"value": None, "display": "New", "tone": "new"}
    delta = current / base - 1.0
    if delta > 0:
        tone = "positive"
    elif delta < 0:
        tone = "negative"
    else:
        tone = "neutral"
    return {
        "value": float(delta),
        "display": f"{delta * 100:.1f}%",
        "tone": tone,
    }


def _metric_cell(value: float, kind: str) -> dict[str, Any]:
    numeric_value = float(value or 0.0)
    if kind == "volume":
        return {"value": numeric_value, "display": f"{numeric_value:,.0f}", "tone": "neutral"}
    if kind == "share":
        return {"value": numeric_value, "display": f"{numeric_value * 100:.1f}%", "tone": "neutral"}
    if kind == "percent":
        return {"value": numeric_value, "display": f"{numeric_value * 100:.1f}%", "tone": "neutral"}
    raise ValueError(f"Unsupported metric cell kind: {kind}")


def _safe_share(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator / denominator)


def _ensure_numeric_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    return frame


@lru_cache(maxsize=8)
def _country_options(dataset_token: str) -> list[dict[str, str]]:
    columns = _resolve_columns(dataset_token)
    selected_columns = [columns.country_value]
    if columns.country_label and columns.country_label != columns.country_value:
        selected_columns.append(columns.country_label)
    table = repo._open_dataset().to_table(columns=selected_columns)
    frame = table.to_pandas().fillna("")
    if columns.country_label and columns.country_label in frame.columns:
        pairs = (
            frame[[columns.country_value, columns.country_label]]
            .drop_duplicates()
            .sort_values([columns.country_label, columns.country_value])
        )
    else:
        pairs = (
            frame[[columns.country_value]]
            .drop_duplicates()
            .assign(**{columns.country_value: lambda df: df[columns.country_value].astype(str)})
            .sort_values(columns.country_value)
        )
        pairs["__label"] = pairs[columns.country_value]
        return [
            {"value": str(row[columns.country_value]).strip(), "label": str(row["__label"]).strip()}
            for _, row in pairs.iterrows()
            if str(row[columns.country_value]).strip()
        ]
    return [
        {
            "value": str(row[columns.country_value]).strip(),
            "label": str(row[columns.country_label]).strip() or str(row[columns.country_value]).strip(),
        }
        for _, row in pairs.iterrows()
        if str(row[columns.country_value]).strip()
    ]


def _available_periods(columns: ColumnMap) -> list[str]:
    return [_month_column_to_period(column) for column in columns.month_columns]


def _window_periods(available_periods: list[str], resolved_period: str, size: int) -> list[str]:
    if resolved_period not in available_periods:
        return available_periods[-size:]
    end_index = available_periods.index(resolved_period) + 1
    start_index = max(0, end_index - max(1, int(size)))
    return available_periods[start_index:end_index]


def _window_periods_if_present(
    available_periods: list[str],
    target_period: str | None,
    size: int,
) -> list[str]:
    if not target_period or target_period not in available_periods:
        return []
    end_index = available_periods.index(target_period) + 1
    start_index = max(0, end_index - max(1, int(size)))
    return available_periods[start_index:end_index]


def _ytd_periods(available_periods: list[str], target_period: str) -> list[str]:
    target = pd.Period(target_period, freq="M")
    return [
        period
        for period in available_periods
        if pd.Period(period, freq="M").year == target.year
        and pd.Period(period, freq="M").month <= target.month
    ]


def _normalize_period_range(
    available_periods: list[str],
    time_range: dict[str, str] | None,
    resolved_period: str,
) -> list[str] | None:
    if not time_range:
        return None
    start = str(time_range.get("start") or "").strip()
    end = str(time_range.get("end") or "").strip() or resolved_period
    if not start and not end:
        return None
    start_period = start if start in available_periods else available_periods[0]
    end_period = end if end in available_periods else resolved_period
    start_index = available_periods.index(start_period)
    end_index = available_periods.index(end_period)
    if start_index > end_index:
        start_index, end_index = end_index, start_index
    periods = available_periods[start_index:end_index + 1]
    if len(periods) <= 1:
        return None
    return periods


def _shifted_periods_if_present(
    periods: list[str],
    available_periods: list[str],
    delta_months: int,
) -> list[str]:
    shifted: list[str] = []
    for period in periods:
        candidate = _shift_period(period, delta_months)
        if candidate in available_periods:
            shifted.append(candidate)
    return shifted


def _normalize_positioning_sales_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in POSITIONING_SALES_MODES else "month"


def _resolve_positioning_sales_window(
    available_periods: list[str],
    resolved_period: str,
    sales_mode: str,
    custom_periods: list[str] | None = None,
) -> tuple[list[str], str, str]:
    if custom_periods:
        return custom_periods, "自定义区间", "Custom Range Sales"
    normalized_mode = _normalize_positioning_sales_mode(sales_mode)
    if normalized_mode == "ytd":
        periods = _ytd_periods(available_periods, resolved_period)
        return periods, "YTD", "YTD Sales"
    if normalized_mode == "rolling12":
        periods = _window_periods(available_periods, resolved_period, 12)
        return periods, "近12个月", "Rolling 12M Sales"
    return [resolved_period], "当月", "Current Month Sales"


def _series_sum(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    present_columns = [column for column in columns if column in frame.columns]
    if not present_columns:
        return pd.Series(0.0, index=frame.index)
    return frame[present_columns].sum(axis=1)


def _ranked_index(series: pd.Series) -> list[tuple[str, float]]:
    cleaned = series.fillna(0.0)
    cleaned = cleaned[cleaned > 0].sort_values(ascending=False)
    return [(str(index), float(value)) for index, value in cleaned.items()]


def _volume_by_group(frame: pd.DataFrame, group_column: str, period_columns: list[str]) -> pd.DataFrame:
    present_columns = [column for column in period_columns if column in frame.columns]
    if frame.empty or not present_columns:
        return pd.DataFrame(columns=present_columns)
    grouped = frame.groupby(group_column, dropna=False)[present_columns].sum(numeric_only=True)
    return grouped.fillna(0.0)


def _total_volume(frame: pd.DataFrame, period_columns: list[str]) -> float:
    present_columns = [column for column in period_columns if column in frame.columns]
    if not present_columns or frame.empty:
        return 0.0
    return float(frame[present_columns].sum(axis=1).sum())


def _build_brand_ranking_items(
    frame: pd.DataFrame,
    current_columns: list[str],
    prior_columns: list[str],
    prior_month_columns: list[str],
    ranking_limit: int,
    *,
    include_model_breakdown: bool = False,
) -> list[dict[str, Any]]:
    current_series = _series_sum(frame, current_columns)
    prior_series = _series_sum(frame, prior_columns)
    prior_month_series = _series_sum(frame, prior_month_columns)
    summary_data: dict[str, Any] = {
        "brand": frame["__brand"],
        "current": current_series,
        "prior": prior_series,
        "prior_month": prior_month_series,
    }
    if include_model_breakdown and "__model" in frame.columns:
        summary_data["model"] = frame["__model"]
    if include_model_breakdown and "__powertrain" in frame.columns:
        summary_data["powertrain"] = frame["__powertrain"]
    summary = pd.DataFrame(summary_data)
    grouped = summary.groupby("brand", dropna=False)[["current", "prior", "prior_month"]].sum(numeric_only=True)
    grouped = grouped[grouped["current"] > 0].sort_values("current", ascending=False)
    total_current = float(grouped["current"].sum()) or 0.0
    grouped = grouped.head(max(1, int(ranking_limit)))
    model_grouped = (
        summary.groupby(["brand", "model", "powertrain"], dropna=False)[["current"]].sum(numeric_only=True)
        if include_model_breakdown and "model" in summary.columns and "powertrain" in summary.columns else
        None
    )
    items: list[dict[str, Any]] = []
    for rank, (brand, row) in enumerate(grouped.iterrows(), start=1):
        current_value = float(row["current"])
        prior_value = float(row["prior"])
        prior_month_value = float(row["prior_month"])
        item = {
            "rank": rank,
            "brand": str(brand),
            "volume": current_value,
            "sharePct": _safe_share(current_value, total_current),
            "shareDisplay": f"{_safe_share(current_value, total_current) * 100:.1f}%",
            "priorVolume": prior_value,
            "priorMonthVolume": prior_month_value,
            "mom": _delta_payload(current_value, prior_month_value),
            "yoy": _delta_payload(current_value, prior_value),
            "barPct": _safe_share(current_value, float(grouped["current"].max()) or 1.0),
        }
        if include_model_breakdown and model_grouped is not None:
            item["modelBreakdown"] = _build_brand_model_breakdown(
                model_grouped,
                brand=str(brand),
                brand_total=current_value,
                limit=MONTHLY_BRAND_MODEL_STACK_LIMIT,
            )
        items.append(item)
    return items


def _build_brand_model_breakdown(
    model_grouped: pd.DataFrame,
    *,
    brand: str,
    brand_total: float,
    limit: int,
) -> list[dict[str, Any]]:
    if brand_total <= 0 or model_grouped.empty:
        return []

    try:
        brand_rows = model_grouped.xs(brand, level="brand")
    except KeyError:
        return []

    if "current" not in brand_rows:
        return []

    model_series = brand_rows.groupby(level="model")["current"].sum()
    ranked_models = _ranked_index(model_series)
    if not ranked_models:
        return []

    items: list[dict[str, Any]] = []
    remaining_volume = 0.0
    for index, (model_name, volume) in enumerate(ranked_models):
        if index >= limit:
            remaining_volume += float(volume)
            continue
        normalized_model = str(model_name).strip() or "Unknown"
        current_value = float(volume)
        model_rows = brand_rows.xs(model_name, level="model")
        if isinstance(model_rows, pd.Series):
            powertrain_series = model_rows
        else:
            powertrain_series = model_rows["current"]
        ranked_powertrains = _ranked_index(powertrain_series)
        dominant_powertrain = (
            str(ranked_powertrains[0][0]).strip().upper()
            if ranked_powertrains else
            "OTHER"
        )
        items.append(
            {
                "model": normalized_model,
                "volume": current_value,
                "sharePct": _safe_share(current_value, brand_total),
                "powertrain": dominant_powertrain,
            }
        )

    if remaining_volume > 0:
        items.append(
            {
                "model": "Other",
                "volume": remaining_volume,
                "sharePct": _safe_share(remaining_volume, brand_total),
                "powertrain": "OTHER",
            }
        )
    return items


def _top_n_fuels(
    frame: pd.DataFrame,
    segment_value: str,
    available_periods: list[str],
    resolved_period: str,
    n: int = 2,
) -> tuple[str, ...]:
    """Return the top *n* powertrain types by YTD volume for *segment_value*."""
    seg = frame[frame["__segment_raw"] == segment_value]
    if seg.empty:
        return FALLBACK_DRILLDOWN_FUELS[:n]
    ytd_cols = [_period_to_month_column(p) for p in _ytd_periods(available_periods, resolved_period)]
    present = [c for c in ytd_cols if c in seg.columns]
    if not present:
        return FALLBACK_DRILLDOWN_FUELS[:n]
    totals = (
        seg.groupby("__powertrain")[present]
        .sum()
        .sum(axis=1)
        .sort_values(ascending=False)
    )
    totals = totals[totals.index != "OTHER"]
    top = tuple(str(idx) for idx in totals.head(n).index)
    return top if len(top) >= n else FALLBACK_DRILLDOWN_FUELS[:n]


def _available_fuel_types(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return list(DEFAULT_FUEL_TYPES)
    discovered = sorted({str(value).strip() for value in frame["__powertrain"].dropna().unique() if str(value).strip() and str(value).strip() != "OTHER"})
    ordered = [fuel for fuel in DEFAULT_FUEL_TYPES if fuel in discovered]
    extras = [fuel for fuel in discovered if fuel not in ordered]
    return ordered + extras


def _normalize_selected_fuels(requested_fuels: list[str], available_fuels: list[str]) -> list[str]:
    normalized = [str(value).strip().upper() for value in requested_fuels if str(value).strip()]
    if normalized:
        selected = [fuel for fuel in available_fuels if fuel in normalized]
        if selected:
            return selected
    fallback = [fuel for fuel in DEFAULT_FUEL_TYPES if fuel in available_fuels]
    return fallback or available_fuels[:]


def _available_body_types(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "__body_type" not in frame.columns:
        return []
    discovered = sorted({
        str(v).strip() for v in frame["__body_type"].dropna().unique()
        if str(v).strip()
    })
    return discovered


def _available_drive_types(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "__drive_type" not in frame.columns:
        return []
    discovered = sorted({
        str(v).strip() for v in frame["__drive_type"].dropna().unique()
        if str(v).strip()
    })
    return discovered


def _build_model_option_list(
    frame: pd.DataFrame,
    column: str,
    sales_column: str,
) -> list[dict[str, str]]:
    if frame.empty or column not in frame.columns or sales_column not in frame.columns:
        return []
    grouped = (
        frame.groupby(column, dropna=False)[sales_column]
        .sum()
        .sort_values(ascending=False)
    )
    return [
        {"value": str(key), "label": str(key)}
        for key in grouped.index
        if str(key).strip()
    ]


def _resolve_positioning_price_band_size(series: pd.Series) -> int:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    cleaned = cleaned[cleaned > 0]
    if cleaned.empty:
        return 5000
    span = float(cleaned.max() - cleaned.min())
    if span <= 25_000:
        return 2_500
    if span <= 60_000:
        return 5_000
    if span <= 120_000:
        return 10_000
    if span <= 250_000:
        return 20_000
    if span <= 600_000:
        return 50_000
    return 100_000


def _format_price_band_label(start: float, end: float) -> str:
    return f"{start:,.0f}-{end:,.0f}"


def _resolve_positioning_price_window(
    series: pd.Series,
    *,
    requested_min: float | None,
    requested_max: float | None,
    band_size: int,
) -> tuple[float, float]:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    cleaned = cleaned[cleaned > 0]
    if cleaned.empty:
        fallback_min = float(requested_min or 0.0)
        fallback_max = float(requested_max or fallback_min + band_size)
        return fallback_min, max(fallback_min + band_size, fallback_max)

    auto_min = float(cleaned.min())
    auto_max = float(cleaned.max())
    resolved_min = float(requested_min) if requested_min is not None else float(band_size * int(auto_min // band_size))
    resolved_max = float(requested_max) if requested_max is not None else float(band_size * int(auto_max // band_size) + band_size)
    if resolved_max <= resolved_min:
        resolved_max = resolved_min + band_size
    return resolved_min, resolved_max


def _resolve_positioning_length_window(
    series: pd.Series,
    *,
    requested_min: float | None,
    requested_max: float | None,
) -> tuple[float, float]:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    cleaned = cleaned[cleaned > 0]
    fallback_min = float(requested_min or 0.0)
    fallback_max = float(requested_max) if requested_max is not None else fallback_min + 1.0
    if cleaned.empty:
        return fallback_min, max(fallback_min + 1.0, fallback_max)

    resolved_min = float(requested_min) if requested_min is not None else float(cleaned.min())
    resolved_max = float(requested_max) if requested_max is not None else float(cleaned.max())
    if resolved_max <= resolved_min:
        resolved_max = resolved_min + 1.0
    return resolved_min, resolved_max


def _filter_positioning_length_window(
    frame: pd.DataFrame,
    *,
    length_min: float | None,
    length_max: float | None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    if "__length" not in frame.columns:
        resolved_min, resolved_max = _resolve_positioning_length_window(
            pd.Series(dtype=float),
            requested_min=length_min,
            requested_max=length_max,
        )
        return frame.copy(), {"min": resolved_min, "max": resolved_max}

    resolved_min, resolved_max = _resolve_positioning_length_window(
        frame["__length"],
        requested_min=length_min,
        requested_max=length_max,
    )
    if frame.empty:
        return frame.copy(), {"min": resolved_min, "max": resolved_max}
    filtered = frame[
        (frame["__length"] >= resolved_min)
        & (frame["__length"] <= resolved_max)
    ].copy()
    return filtered, {"min": resolved_min, "max": resolved_max}


def _positioning_page_rows(
    frame: pd.DataFrame,
    page_key: str,
) -> pd.DataFrame:
    if page_key == "overview":
        return frame.copy()
    if page_key == "suvAll":
        return frame[frame["__segment_raw"].str.startswith("SUV", na=False)].copy()
    if page_key == "suvA0":
        return frame[frame["__segment_raw"] == "SUV A0"].copy()
    if page_key == "suvA":
        return frame[frame["__segment_raw"] == "SUV A"].copy()
    return frame[
        frame["__segment_raw"].str.startswith(("SUV B", "SUV C", "SUV D"), na=False)
    ].copy()


def _resolve_version_comparison_segment(
    frame: pd.DataFrame,
    requested_segment: str | None,
    *,
    sales_column: str,
) -> tuple[str, list[dict[str, str]]]:
    if frame.empty or sales_column not in frame.columns:
        return "", []
    grouped = (
        frame.groupby("__segment_raw", dropna=False)[sales_column]
        .sum()
        .sort_values(ascending=False)
    )
    options = [
        {"value": str(segment), "label": str(segment)}
        for segment in grouped.index
        if str(segment).strip()
    ]
    if not options:
        return "", []
    available = {option["value"] for option in options}
    if requested_segment and requested_segment in available:
        return requested_segment, options
    preferred = next((option["value"] for option in options if option["value"] == "SUV B"), None)
    return preferred or options[0]["value"], options


def _build_all_model_options(
    frame: pd.DataFrame,
    sales_column: str,
) -> list[dict[str, str]]:
    if frame.empty or sales_column not in frame.columns:
        return []
    grouped = (
        frame.groupby("__model", dropna=False)[sales_column]
        .sum()
        .sort_values(ascending=False)
    )
    return [
        {"value": str(model), "label": str(model)}
        for model in grouped.index
        if str(model).strip()
    ]


def _resolve_version_comparison_models(
    frame: pd.DataFrame,
    requested_models: list[str],
    *,
    sales_column: str,
) -> tuple[list[str], list[dict[str, str]]]:
    if frame.empty or sales_column not in frame.columns:
        return [], []
    grouped = (
        frame.groupby("__model", dropna=False)[sales_column]
        .sum()
        .sort_values(ascending=False)
    )
    options = [
        {"value": str(model), "label": str(model)}
        for model in grouped.index
        if str(model).strip()
    ]
    if not options:
        return [], []
    available = {option["value"] for option in options}
    selected: list[str] = []
    seen: set[str] = set()
    for model in requested_models:
        normalized = str(model).strip()
        if not normalized or normalized not in available or normalized in seen:
            continue
        selected.append(normalized)
        seen.add(normalized)
        if len(selected) >= VERSION_COMPARISON_MODEL_LIMIT:
            break
    if selected:
        return selected, options
    return [option["value"] for option in options[:3]], options


def _build_version_comparison_bubble_items(
    frame: pd.DataFrame,
    *,
    sales_column: str,
    msrp_min: float | None = None,
    msrp_max: float | None = None,
) -> list[dict[str, Any]]:
    if frame.empty or sales_column not in frame.columns:
        return []
    working = frame.copy()
    working["__sales"] = pd.to_numeric(working[sales_column], errors="coerce").fillna(0.0)
    working = working[working["__sales"] > 0]
    if working.empty:
        return []
    grouped = working.groupby(
        ["__model", "__version", "__trim", "__powertrain"],
        dropna=False,
    )
    aggregated = grouped.agg(
        Length=("__length", "median"),
        MSRP=("__msrp", "median"),
        MsrpMin=("__msrp", "min"),
        MsrpMax=("__msrp", "max"),
        Sales=("__sales", "sum"),
        VariantCount=("__version", "size"),
    ).reset_index()
    aggregated = aggregated[
        (aggregated["Length"] > 0)
        & (aggregated["MSRP"] > 0)
        & (aggregated["Sales"] > 0)
    ].copy()
    if aggregated.empty:
        return []
    if msrp_min is not None:
        aggregated = aggregated[aggregated["MSRP"] >= float(msrp_min)]
    if msrp_max is not None:
        aggregated = aggregated[aggregated["MSRP"] <= float(msrp_max)]
    if aggregated.empty:
        return []
    aggregated = aggregated.sort_values(["Sales", "MSRP"], ascending=[False, True])
    return [
        {
            "model": str(row["__model"]),
            "version": str(row["__version"]),
            "trim": str(row["__trim"]),
            "powertrain": str(row["__powertrain"]),
            "length": float(row["Length"]),
            "msrp": float(row["MSRP"]),
            "msrpMin": float(row["MsrpMin"]),
            "msrpMax": float(row["MsrpMax"]),
            "sales": float(row["Sales"]),
            "variantCount": int(row["VariantCount"]),
        }
        for _, row in aggregated.iterrows()
    ]


def _build_version_comparison_page_payload(
    frame: pd.DataFrame,
    *,
    title: str,
    subtitle: str,
    sales_column: str,
    sales_metric_label: str,
    sales_metric_detail: str,
    selected_fuels: list[str],
    msrp_min: float | None,
    msrp_max: float | None,
    price_band_size: int | None,
) -> dict[str, Any]:
    price_bands = _build_positioning_price_bands(
        frame,
        sales_column=sales_column,
        selected_fuels=selected_fuels,
        msrp_min=msrp_min,
        msrp_max=msrp_max,
        band_size=price_band_size,
    )
    range_min = price_bands["range"]["min"]
    range_max = price_bands["range"]["max"]
    range_frame = frame[
        (frame["__msrp"] >= range_min)
        & (frame["__msrp"] <= range_max)
    ].copy()
    bubble_items = _build_version_comparison_bubble_items(
        range_frame,
        sales_column=sales_column,
        msrp_min=range_min,
        msrp_max=range_max,
    )
    total_sales = float(pd.to_numeric(range_frame[sales_column], errors="coerce").fillna(0.0).sum()) if sales_column in range_frame.columns else 0.0
    model_count = int(range_frame["__model"].nunique()) if "__model" in range_frame.columns else 0
    version_count = len(bubble_items)
    lead_version = bubble_items[0]["version"] if bubble_items else "-"
    return {
        "title": title,
        "subtitle": subtitle,
        "summaryText": (
            f"{subtitle} 当前共 {total_sales:,.0f} 台，覆盖 {model_count} 个 Model / {version_count} 个版型。"
        ) if total_sales > 0 else f"{subtitle} 当前筛选下暂无可用数据。",
        "metrics": [
            {"label": sales_metric_label, "value": total_sales, "detail": sales_metric_detail},
            {"label": "Compared Models", "value": model_count, "detail": "当前对比 Model 数"},
            {"label": "Visible Versions", "value": version_count, "detail": "右侧版型气泡数"},
            {"label": "Lead Version", "value": lead_version, "detail": "销量最高版型"},
        ],
        "priceBands": price_bands,
        "bubbleChart": {
            "items": bubble_items,
        },
    }


def _build_positioning_price_bands(
    frame: pd.DataFrame,
    *,
    sales_column: str,
    selected_fuels: list[str],
    msrp_min: float | None = None,
    msrp_max: float | None = None,
    band_size: int | None = None,
) -> dict[str, Any]:
    if frame.empty or sales_column not in frame.columns:
        return {"bandSize": int(band_size or 5000), "range": {"min": float(msrp_min or 0.0), "max": float(msrp_max or 0.0)}, "items": []}

    working = frame.copy()
    working["__sales"] = pd.to_numeric(working[sales_column], errors="coerce").fillna(0.0)
    working = working[working["__sales"] > 0]
    if working.empty:
        resolved_band_size = int(band_size or 5000)
        resolved_min, resolved_max = _resolve_positioning_price_window(
            frame["__msrp"],
            requested_min=msrp_min,
            requested_max=msrp_max,
            band_size=resolved_band_size,
        )
        return {"bandSize": resolved_band_size, "range": {"min": resolved_min, "max": resolved_max}, "items": []}

    resolved_band_size = int(band_size or _resolve_positioning_price_band_size(working["__msrp"]))
    resolved_min, resolved_max = _resolve_positioning_price_window(
        working["__msrp"],
        requested_min=msrp_min,
        requested_max=msrp_max,
        band_size=resolved_band_size,
    )
    working = working[
        (working["__msrp"] >= resolved_min)
        & (working["__msrp"] <= resolved_max)
    ].copy()
    if working.empty:
        return {"bandSize": resolved_band_size, "range": {"min": resolved_min, "max": resolved_max}, "items": []}

    working["__band_index"] = ((working["__msrp"] - resolved_min) // resolved_band_size).astype(int)
    working["__band_start"] = resolved_min + working["__band_index"] * resolved_band_size

    grouped = (
        working.groupby(["__band_start", "__powertrain"], dropna=False)["__sales"]
        .sum()
        .reset_index()
    )
    band_starts: list[float] = []
    cursor = resolved_min
    while cursor < resolved_max:
        band_starts.append(float(cursor))
        cursor += resolved_band_size
    if not band_starts:
        band_starts = [resolved_min]
    items: list[dict[str, Any]] = []
    for band_start in band_starts:
        bucket = grouped[grouped["__band_start"] == band_start]
        fuel_mix = {
            fuel: float(
                bucket.loc[bucket["__powertrain"] == fuel, "__sales"].sum()
            )
            for fuel in selected_fuels
        }
        total_sales = float(sum(fuel_mix.values()))
        band_end = min(float(band_start + resolved_band_size), resolved_max)
        items.append(
            {
                "bandStart": float(band_start),
                "bandEnd": band_end,
                "label": _format_price_band_label(band_start, band_end),
                "bandMid": float((band_start + band_end) / 2),
                "bandWidth": float(max(band_end - band_start, resolved_band_size)),
                "sales": total_sales,
                "fuelMix": fuel_mix,
            }
        )
    return {"bandSize": resolved_band_size, "range": {"min": resolved_min, "max": resolved_max}, "items": items}


def _build_positioning_bubble_items(
    frame: pd.DataFrame,
    *,
    sales_column: str,
    bubble_limit: int = POSITIONING_BUBBLE_LIMIT,
    msrp_min: float | None = None,
    msrp_max: float | None = None,
    length_min: float | None = None,
    length_max: float | None = None,
) -> list[dict[str, Any]]:
    if frame.empty or sales_column not in frame.columns:
        return []

    working = frame.copy()
    working["__sales"] = pd.to_numeric(working[sales_column], errors="coerce").fillna(0.0)
    working = working[working["__sales"] > 0]
    if working.empty:
        return []

    grouped = working.groupby(
        ["__brand", "__model", "__powertrain", "__segment_raw"],
        dropna=False,
    )
    aggregated = grouped.agg(
        Length=("__length", "median"),
        MSRP=("__msrp", "median"),
        MsrpMin=("__msrp", "min"),
        MsrpMax=("__msrp", "max"),
        Sales=("__sales", "sum"),
        VariantCount=("__model", "size"),
    ).reset_index()
    aggregated = aggregated[
        (aggregated["Length"] > 0)
        & (aggregated["MsrpMin"] > 0)
        & (aggregated["Sales"] > 0)
    ].copy()
    if aggregated.empty:
        return []
    if msrp_min is not None:
        aggregated = aggregated[aggregated["MsrpMin"] >= float(msrp_min)]
    if msrp_max is not None:
        aggregated = aggregated[aggregated["MsrpMin"] <= float(msrp_max)]
    if length_min is not None:
        aggregated = aggregated[aggregated["Length"] >= float(length_min)]
    if length_max is not None:
        aggregated = aggregated[aggregated["Length"] <= float(length_max)]
    if aggregated.empty:
        return []
    aggregated = aggregated.sort_values(["Sales", "MsrpMin"], ascending=[False, True]).head(max(1, int(bubble_limit)))
    return [
        {
            "brand": str(row["__brand"]),
            "model": str(row["__model"]),
            "powertrain": str(row["__powertrain"]),
            "segment": str(row["__segment_raw"]),
            "length": float(row["Length"]),
            "msrp": float(row["MSRP"]),
            "msrpMin": float(row["MsrpMin"]),
            "msrpMax": float(row["MsrpMax"]),
            "sales": float(row["Sales"]),
            "variantCount": int(row["VariantCount"]),
        }
        for _, row in aggregated.iterrows()
    ]


def _build_positioning_page_payload(
    frame: pd.DataFrame,
    *,
    page_key: str,
    title: str,
    subtitle: str,
    sales_column: str,
    sales_metric_label: str,
    sales_metric_detail: str,
    selected_fuels: list[str],
    top_n: int,
    msrp_min: float | None,
    msrp_max: float | None,
    price_band_size: int | None,
    length_min: float | None = None,
    length_max: float | None = None,
) -> dict[str, Any]:
    page_frame, length_range = _filter_positioning_length_window(
        _positioning_page_rows(frame, page_key),
        length_min=length_min,
        length_max=length_max,
    )
    price_bands = _build_positioning_price_bands(
        page_frame,
        sales_column=sales_column,
        selected_fuels=selected_fuels,
        msrp_min=msrp_min,
        msrp_max=msrp_max,
        band_size=price_band_size,
    )
    range_min = price_bands["range"]["min"]
    range_max = price_bands["range"]["max"]
    range_frame = page_frame[
        (page_frame["__msrp"] >= range_min)
        & (page_frame["__msrp"] <= range_max)
    ].copy()
    bubble_items = _build_positioning_bubble_items(
        range_frame,
        sales_column=sales_column,
        bubble_limit=top_n,
        msrp_min=range_min,
        msrp_max=range_max,
        length_min=length_range["min"],
        length_max=length_range["max"],
    )
    total_sales = float(pd.to_numeric(range_frame[sales_column], errors="coerce").fillna(0.0).sum()) if sales_column in range_frame.columns else 0.0
    model_count = int(range_frame["__model"].nunique()) if "__model" in range_frame.columns else 0
    min_msrp = float(range_frame["__msrp"].min()) if not range_frame.empty else range_min
    max_msrp = float(range_frame["__msrp"].max()) if not range_frame.empty else range_max
    min_length = float(range_frame["__length"].min()) if not range_frame.empty else length_range["min"]
    max_length = float(range_frame["__length"].max()) if not range_frame.empty else length_range["max"]
    top_fuel = "-"
    if not range_frame.empty:
        fuel_totals = (
            range_frame.groupby("__powertrain", dropna=False)[sales_column]
            .sum()
            .sort_values(ascending=False)
        )
        if not fuel_totals.empty:
            top_fuel = str(fuel_totals.index[0])
    lead_model = bubble_items[0]["model"] if bubble_items else "-"
    return {
        "key": page_key,
        "title": title,
        "subtitle": subtitle,
        "summaryText": (
            f"{subtitle} 当前共 {total_sales:,.0f} 台，主导动力 {top_fuel}，"
            f"车长覆盖 {min_length:,.0f}-{max_length:,.0f} mm，"
            f"最低 MSRP 覆盖 {min_msrp:,.0f}-{max_msrp:,.0f}。"
        ) if total_sales > 0 else f"{subtitle} 当前筛选下暂无可用数据。",
        "metrics": [
            {"label": sales_metric_label, "value": total_sales, "detail": sales_metric_detail},
            {"label": "Tracked Models", "value": model_count, "detail": "纳入定位车型数"},
            {"label": "Lowest MSRP", "value": min_msrp, "detail": "当前页最低价格"},
            {"label": "Lead Model", "value": lead_model, "detail": "销量最高气泡"},
        ],
        "lengthRange": length_range,
        "priceBands": price_bands,
        "bubbleChart": {
            "items": bubble_items,
            "bubbleLimit": int(top_n),
        },
    }


def _build_overview_payload(
    frame: pd.DataFrame,
    selected_fuels: list[str],
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    ranking_limit: int,
    custom_range_periods: list[str] | None = None,
) -> dict[str, Any]:
    trend_periods = _window_periods(available_periods, resolved_period, 24)
    trend_columns = [_period_to_month_column(period) for period in trend_periods]
    grouped = _volume_by_group(frame, "__powertrain", trend_columns)
    grouped = grouped.reindex(selected_fuels, fill_value=0.0)
    suv_frame = (
        frame[frame["__segment_raw"].str.startswith("SUV", na=False)].copy()
        if "__segment_raw" in frame.columns
        else frame.iloc[0:0].copy()
    )
    suv_grouped = _volume_by_group(suv_frame, "__powertrain", trend_columns)
    suv_grouped = suv_grouped.reindex(selected_fuels, fill_value=0.0)

    total_by_column = {column: float(grouped[column].sum()) for column in trend_columns if column in grouped.columns}
    suv_total_by_column = {
        column: float(suv_grouped[column].sum()) for column in trend_columns if column in suv_grouped.columns
    }
    trend_items: list[dict[str, Any]] = []
    for period in trend_periods:
        column = _period_to_month_column(period)
        current_total = total_by_column.get(column, 0.0)
        suv_total = suv_total_by_column.get(column, 0.0)
        prior_total = total_by_column.get(_period_to_month_column(prior_period), 0.0) if prior_period else 0.0
        same_month_total = total_by_column.get(_period_to_month_column(same_month_last_year_period), 0.0) if same_month_last_year_period else 0.0
        if period != resolved_period:
            period_prior = _shift_period(period, -1)
            period_same_month = _shift_period(period, -12)
            prior_total = total_by_column.get(_period_to_month_column(period_prior), 0.0) if period_prior in available_periods else 0.0
            same_month_total = total_by_column.get(_period_to_month_column(period_same_month), 0.0) if period_same_month in available_periods else 0.0
        trend_items.append(
            {
                "period": period,
                "label": _short_period_label(period),
                "totalVolume": current_total,
                "fuelMix": {
                    fuel: float(grouped.at[fuel, column]) if fuel in grouped.index and column in grouped.columns else 0.0
                    for fuel in selected_fuels
                },
                "suvTotalVolume": suv_total,
                "suvFuelMix": {
                    fuel: float(suv_grouped.at[fuel, column]) if fuel in suv_grouped.index and column in suv_grouped.columns else 0.0
                    for fuel in selected_fuels
                },
                "mom": _delta_payload(current_total, prior_total),
                "yoy": _delta_payload(current_total, same_month_total),
            }
        )

    current_period_columns = [_period_to_month_column(resolved_period)]
    prior_period_columns = [_period_to_month_column(prior_period)] if prior_period else []
    same_month_columns = [_period_to_month_column(same_month_last_year_period)] if same_month_last_year_period else []
    current_ytd_periods = _ytd_periods(available_periods, resolved_period)
    prior_ytd_periods = _ytd_periods(available_periods, _shift_period(resolved_period, -12)) if same_month_last_year_period else []
    current_ytd_columns = [_period_to_month_column(period) for period in current_ytd_periods]
    prior_ytd_columns = [_period_to_month_column(period) for period in prior_ytd_periods]
    current_rolling12_periods = _window_periods_if_present(available_periods, resolved_period, 12)
    prior_rolling12_periods = _window_periods_if_present(available_periods, _shift_period(resolved_period, -12), 12)
    current_rolling12_columns = [_period_to_month_column(period) for period in current_rolling12_periods]
    prior_rolling12_columns = [_period_to_month_column(period) for period in prior_rolling12_periods]
    custom_range_columns = [_period_to_month_column(period) for period in (custom_range_periods or [])]
    prior_custom_range_periods = (
        _shifted_periods_if_present(custom_range_periods, available_periods, -12)
        if custom_range_periods
        else []
    )
    prior_custom_range_columns = [_period_to_month_column(period) for period in prior_custom_range_periods]

    current_month_total = _total_volume(frame, current_period_columns)
    same_month_total = _total_volume(frame, same_month_columns)
    current_ytd_total = _total_volume(frame, current_ytd_columns)
    prior_ytd_total = _total_volume(frame, prior_ytd_columns)
    current_rolling12_total = _total_volume(frame, current_rolling12_columns)
    prior_rolling12_total = _total_volume(frame, prior_rolling12_columns)
    custom_range_total = _total_volume(frame, custom_range_columns)
    prior_custom_range_total = _total_volume(frame, prior_custom_range_columns)

    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)

    payload = {
        "summary": {
            "headline": f"{_short_period_label(resolved_period)} 总销量 {current_month_total:,.0f} 台，YoY { _delta_payload(current_month_total, same_month_total)['display'] }",
            "subheadline": f"近12个月总销量 {current_rolling12_total:,.0f} 台，Rolling 12M YoY { _delta_payload(current_rolling12_total, prior_rolling12_total)['display'] }",
            "currentMonthVolume": current_month_total,
            "currentMonthYoY": _delta_payload(current_month_total, same_month_total),
            "rolling12Volume": current_rolling12_total,
            "rolling12YoY": _delta_payload(current_rolling12_total, prior_rolling12_total),
            "ytdVolume": current_ytd_total,
            "ytdYoY": _delta_payload(current_ytd_total, prior_ytd_total),
        },
        "trend": {
            "periods": trend_periods,
            "items": trend_items,
        },
        "ytdBrandRanking": {
            "title": "YTD Brand Ranking",
            "currentLabel": f"{year_text[2:4]}YTD",
            "priorLabel": f"{str(int(year_text) - 1)[2:4]}YTD",
            "items": _build_brand_ranking_items(
                frame,
                current_columns=current_ytd_columns,
                prior_columns=prior_ytd_columns,
                prior_month_columns=same_month_columns,
                ranking_limit=ranking_limit,
                include_model_breakdown=True,
            ),
        },
        "rolling12BrandRanking": {
            "title": "Rolling 12M Brand Ranking",
            "currentLabel": f"L12M {_short_period_label(resolved_period)}",
            "priorLabel": f"L12M {_short_period_label(_shift_period(resolved_period, -12))}",
            "items": _build_brand_ranking_items(
                frame,
                current_columns=current_rolling12_columns,
                prior_columns=prior_rolling12_columns,
                prior_month_columns=[],
                ranking_limit=ranking_limit,
                include_model_breakdown=True,
            ),
        },
        "monthlyBrandRanking": {
            "title": "Monthly Brand Ranking",
            "currentLabel": _short_period_label(resolved_period),
            "priorLabel": _short_period_label(same_month_last_year_period) if same_month_last_year_period else "-",
            "previousMonthLabel": _short_period_label(prior_period) if prior_period else "-",
            "items": _build_brand_ranking_items(
                frame,
                current_columns=current_period_columns,
                prior_columns=same_month_columns,
                prior_month_columns=prior_period_columns,
                ranking_limit=ranking_limit,
                include_model_breakdown=True,
            ),
        },
    }
    if custom_range_periods:
        range_start = custom_range_periods[0]
        range_end = custom_range_periods[-1]
        payload["summary"]["customRangeVolume"] = custom_range_total
        payload["summary"]["customRangeYoY"] = _delta_payload(custom_range_total, prior_custom_range_total)
        payload["summary"]["customRangeLabel"] = (
            _short_period_label(range_start)
            if range_start == range_end
            else f"{_short_period_label(range_start)} - {_short_period_label(range_end)}"
        )
        payload["customRangeBrandRanking"] = {
            "title": "Custom Range Brand Ranking",
            "currentLabel": payload["summary"]["customRangeLabel"],
            "priorLabel": (
                _short_period_label(prior_custom_range_periods[0])
                if len(prior_custom_range_periods) == 1
                else (
                    f"{_short_period_label(prior_custom_range_periods[0])} - {_short_period_label(prior_custom_range_periods[-1])}"
                    if prior_custom_range_periods
                    else "-"
                )
            ),
            "items": _build_brand_ranking_items(
                frame,
                current_columns=custom_range_columns,
                prior_columns=prior_custom_range_columns,
                prior_month_columns=[],
                ranking_limit=ranking_limit,
                include_model_breakdown=True,
            ),
        }
    return payload


def _build_origin_payload(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    origin_window_months: int,
    custom_range_periods: list[str] | None = None,
) -> dict[str, Any]:
    trend_periods = _window_periods(available_periods, resolved_period, origin_window_months)
    trend_columns = [_period_to_month_column(period) for period in trend_periods]
    grouped = _volume_by_group(frame, "__origin", trend_columns)
    ordered_origins = [origin for origin in PRIMARY_ORIGINS if origin in grouped.index]
    ordered_origins.extend([origin for origin in grouped.index if origin not in ordered_origins])

    series: list[dict[str, Any]] = []
    for origin in ordered_origins:
        points: list[dict[str, Any]] = []
        for period in trend_periods:
            column = _period_to_month_column(period)
            month_total = float(grouped[column].sum()) if column in grouped.columns else 0.0
            origin_value = float(grouped.at[origin, column]) if origin in grouped.index and column in grouped.columns else 0.0
            points.append(
                {
                    "period": period,
                    "label": _short_period_label(period),
                    "volume": origin_value,
                    "sharePct": _safe_share(origin_value, month_total),
                }
            )
        series.append({"origin": origin, "points": points})

    current_column = _period_to_month_column(resolved_period)
    prior_column = _period_to_month_column(prior_period) if prior_period else None
    same_month_column = _period_to_month_column(same_month_last_year_period) if same_month_last_year_period else None
    current_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, resolved_period)]
    prior_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, _shift_period(resolved_period, -12))] if same_month_last_year_period else []
    current_rolling12_columns = [
        _period_to_month_column(period)
        for period in _window_periods_if_present(available_periods, resolved_period, 12)
    ]
    prior_rolling12_columns = [
        _period_to_month_column(period)
        for period in _window_periods_if_present(available_periods, _shift_period(resolved_period, -12), 12)
    ]
    custom_range_columns = [_period_to_month_column(period) for period in (custom_range_periods or [])]
    prior_custom_range_periods = (
        _shifted_periods_if_present(custom_range_periods, available_periods, -12)
        if custom_range_periods
        else []
    )
    prior_custom_range_columns = [_period_to_month_column(period) for period in prior_custom_range_periods]

    current_total = float(grouped[current_column].sum()) if current_column in grouped.columns else 0.0
    prior_total = float(grouped[prior_column].sum()) if prior_column and prior_column in grouped.columns else 0.0
    same_month_total = float(grouped[same_month_column].sum()) if same_month_column and same_month_column in grouped.columns else 0.0

    brand_trend_groups: list[dict[str, Any]] = []
    for origin in ordered_origins:
        origin_frame = frame[frame["__origin"] == origin].copy()
        brand_grouped = _volume_by_group(origin_frame, "__brand", trend_columns)
        if brand_grouped.empty:
            continue
        ranked_brands = _ranked_index(brand_grouped[current_column]) if current_column in brand_grouped.columns else []
        selected_brands = [brand for brand, _ in ranked_brands[:ORIGIN_BRAND_TREND_LIMIT]]
        if not selected_brands:
            continue
        brand_series: list[dict[str, Any]] = []
        for brand in selected_brands:
            points: list[dict[str, Any]] = []
            for period in trend_periods:
                column = _period_to_month_column(period)
                month_total = float(brand_grouped[column].sum()) if column in brand_grouped.columns else 0.0
                brand_value = float(brand_grouped.at[brand, column]) if brand in brand_grouped.index and column in brand_grouped.columns else 0.0
                points.append(
                    {
                        "period": period,
                        "label": _short_period_label(period),
                        "volume": brand_value,
                        "sharePct": _safe_share(brand_value, month_total),
                    }
                )
            brand_series.append({"brand": brand, "points": points})
        brand_trend_groups.append({"origin": origin, "series": brand_series})

    summary_frame = pd.DataFrame(index=ordered_origins)
    summary_frame["current"] = grouped[current_column] if current_column in grouped.columns else 0.0
    summary_frame["prior"] = grouped[prior_column] if prior_column and prior_column in grouped.columns else 0.0
    summary_frame["same_month"] = grouped[same_month_column] if same_month_column and same_month_column in grouped.columns else 0.0
    current_ytd_grouped = _volume_by_group(frame, "__origin", current_ytd_columns)
    prior_ytd_grouped = _volume_by_group(frame, "__origin", prior_ytd_columns)
    current_rolling12_grouped = _volume_by_group(frame, "__origin", current_rolling12_columns)
    prior_rolling12_grouped = _volume_by_group(frame, "__origin", prior_rolling12_columns)
    summary_frame["ytd"] = current_ytd_grouped.sum(axis=1) if not current_ytd_grouped.empty else 0.0
    summary_frame["prior_ytd"] = prior_ytd_grouped.sum(axis=1) if not prior_ytd_grouped.empty else 0.0
    summary_frame["rolling12"] = current_rolling12_grouped.sum(axis=1) if not current_rolling12_grouped.empty else 0.0
    summary_frame["prior_rolling12"] = prior_rolling12_grouped.sum(axis=1) if not prior_rolling12_grouped.empty else 0.0
    current_custom_range_grouped = _volume_by_group(frame, "__origin", custom_range_columns)
    prior_custom_range_grouped = _volume_by_group(frame, "__origin", prior_custom_range_columns)
    summary_frame["custom_range"] = current_custom_range_grouped.sum(axis=1) if not current_custom_range_grouped.empty else 0.0
    summary_frame["prior_custom_range"] = prior_custom_range_grouped.sum(axis=1) if not prior_custom_range_grouped.empty else 0.0
    summary_frame = summary_frame.fillna(0.0)

    matrix_rows = []
    for metric_key, label, kind in [
        ("current_volume", "当月销量", "volume"),
        ("mom", "MoM", "delta"),
        ("yoy", "YoY", "delta"),
        ("rolling12", "近12个月", "volume"),
        ("rolling12_yoy", "Rolling 12M YoY", "delta"),
        ("ytd", "YTD", "volume"),
        ("ytd_yoy", "YTD YoY", "delta"),
    ]:
        cells = []
        for origin in ordered_origins:
            row = summary_frame.loc[origin]
            if metric_key == "current_volume":
                payload = _metric_cell(float(row["current"]), kind)
            elif metric_key == "mom":
                payload = _delta_payload(float(row["current"]), float(row["prior"]))
            elif metric_key == "yoy":
                payload = _delta_payload(float(row["current"]), float(row["same_month"]))
            elif metric_key == "rolling12":
                payload = _metric_cell(float(row["rolling12"]), kind)
            elif metric_key == "rolling12_yoy":
                payload = _delta_payload(float(row["rolling12"]), float(row["prior_rolling12"]))
            elif metric_key == "ytd":
                payload = _metric_cell(float(row["ytd"]), kind)
            else:
                payload = _delta_payload(float(row["ytd"]), float(row["prior_ytd"]))
            cells.append({"key": origin, **payload})
        matrix_rows.append({"metricKey": metric_key, "label": label, "cells": cells})

    top_origin = summary_frame.sort_values("current", ascending=False).index[0] if not summary_frame.empty else "市场"
    top_origin_share = _safe_share(float(summary_frame.loc[top_origin, "current"]) if top_origin in summary_frame.index else 0.0, current_total)
    summary_text = (
        f"{top_origin}当月占比 {top_origin_share * 100:.1f}% ，"
        f"MoM { _delta_payload(current_total, prior_total)['display'] }，"
        f"YoY { _delta_payload(current_total, same_month_total)['display'] }。"
    )

    return {
        "summaryText": summary_text,
        "trend": {"series": series},
        "brandTrend": {"groups": brand_trend_groups},
        "matrix": {"columns": ordered_origins, "rows": matrix_rows},
        "customRangeMatrixRow": {
            "metricKey": "custom_range",
            "label": "自定义区间",
            "cells": [
                {"key": origin, **_metric_cell(float(summary_frame.loc[origin, "custom_range"]), "volume")}
                for origin in ordered_origins
            ],
        } if custom_range_periods else None,
        "customRangeYoYMatrixRow": {
            "metricKey": "custom_range_yoy",
            "label": "自定义区间 YoY",
            "cells": [
                {"key": origin, **_delta_payload(float(summary_frame.loc[origin, "custom_range"]), float(summary_frame.loc[origin, "prior_custom_range"]))}
                for origin in ordered_origins
            ],
        } if custom_range_periods else None,
    }


def _build_segment_payload(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    body_window_months: int,
    custom_range_periods: list[str] | None = None,
) -> dict[str, Any]:
    working = frame.copy()
    working["__segment_bucket"] = working["__segment_raw"].map(_segment_matrix_bucket)
    working = working[working["__segment_bucket"].notna()].copy()
    if working.empty:
        return {
            "summaryText": "当前筛选下没有可用的细分市场数据。",
            "matrix": {"columns": list(SEGMENT_MATRIX_ORDER), "rows": []},
            "bodyShareTrend": {"items": []},
            "suvSegmentShareTrend": {"items": []},
            "channelMix": {
                "options": CHANNEL_MIX_OPTIONS,
                "month": {"title": "Monthly Channel Mix", "items": []},
                "ytd": {"title": "YTD Channel Mix", "items": []},
                "rolling12": {"title": "Rolling 12M Channel Mix", "items": []},
                "customRange": None,
            },
        }

    current_column = _period_to_month_column(resolved_period)
    prior_column = _period_to_month_column(prior_period) if prior_period else None
    same_month_column = _period_to_month_column(same_month_last_year_period) if same_month_last_year_period else None
    current_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, resolved_period)]
    prior_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, _shift_period(resolved_period, -12))] if same_month_last_year_period else []
    current_rolling12_columns = [
        _period_to_month_column(period)
        for period in _window_periods_if_present(available_periods, resolved_period, 12)
    ]
    prior_rolling12_columns = [
        _period_to_month_column(period)
        for period in _window_periods_if_present(available_periods, _shift_period(resolved_period, -12), 12)
    ]
    custom_range_columns = [_period_to_month_column(period) for period in (custom_range_periods or [])]
    prior_custom_range_periods = (
        _shifted_periods_if_present(custom_range_periods, available_periods, -12)
        if custom_range_periods
        else []
    )
    prior_custom_range_columns = [_period_to_month_column(period) for period in prior_custom_range_periods]

    grouped_current = _volume_by_group(working, "__segment_bucket", [current_column])
    grouped_prior = _volume_by_group(working, "__segment_bucket", [prior_column] if prior_column else [])
    grouped_same_month = _volume_by_group(working, "__segment_bucket", [same_month_column] if same_month_column else [])
    grouped_ytd = _volume_by_group(working, "__segment_bucket", current_ytd_columns)
    grouped_prior_ytd = _volume_by_group(working, "__segment_bucket", prior_ytd_columns)
    grouped_rolling12 = _volume_by_group(working, "__segment_bucket", current_rolling12_columns)
    grouped_prior_rolling12 = _volume_by_group(working, "__segment_bucket", prior_rolling12_columns)
    grouped_custom_range = _volume_by_group(working, "__segment_bucket", custom_range_columns)
    grouped_prior_custom_range = _volume_by_group(working, "__segment_bucket", prior_custom_range_columns)

    matrix_rows = []
    for metric_key, label in [
        ("current_volume", "当月销量"),
        ("mom", "MoM"),
        ("yoy", "YoY"),
        ("rolling12", "近12个月"),
        ("rolling12_yoy", "Rolling 12M YoY"),
        ("ytd", "YTD"),
        ("ytd_yoy", "YTD YoY"),
    ]:
        cells = []
        for bucket in SEGMENT_MATRIX_ORDER:
            current_value = float(grouped_current.at[bucket, current_column]) if bucket in grouped_current.index and current_column in grouped_current.columns else 0.0
            prior_value = float(grouped_prior.at[bucket, prior_column]) if prior_column and bucket in grouped_prior.index and prior_column in grouped_prior.columns else 0.0
            same_month_value = float(grouped_same_month.at[bucket, same_month_column]) if same_month_column and bucket in grouped_same_month.index and same_month_column in grouped_same_month.columns else 0.0
            ytd_value = float(grouped_ytd.loc[bucket].sum()) if bucket in grouped_ytd.index else 0.0
            prior_ytd_value = float(grouped_prior_ytd.loc[bucket].sum()) if bucket in grouped_prior_ytd.index else 0.0
            rolling12_value = float(grouped_rolling12.loc[bucket].sum()) if bucket in grouped_rolling12.index else 0.0
            prior_rolling12_value = float(grouped_prior_rolling12.loc[bucket].sum()) if bucket in grouped_prior_rolling12.index else 0.0
            custom_range_value = float(grouped_custom_range.loc[bucket].sum()) if bucket in grouped_custom_range.index else 0.0
            prior_custom_range_value = float(grouped_prior_custom_range.loc[bucket].sum()) if bucket in grouped_prior_custom_range.index else 0.0
            if metric_key == "current_volume":
                payload = _metric_cell(current_value, "volume")
            elif metric_key == "mom":
                payload = _delta_payload(current_value, prior_value)
            elif metric_key == "yoy":
                payload = _delta_payload(current_value, same_month_value)
            elif metric_key == "rolling12":
                payload = _metric_cell(rolling12_value, "volume")
            elif metric_key == "rolling12_yoy":
                payload = _delta_payload(rolling12_value, prior_rolling12_value)
            elif metric_key == "ytd":
                payload = _metric_cell(ytd_value, "volume")
            else:
                payload = _delta_payload(ytd_value, prior_ytd_value)
            cells.append({"key": bucket, **payload})
        matrix_rows.append({"metricKey": metric_key, "label": label, "cells": cells})

    if custom_range_periods:
        matrix_rows.extend([
            {
                "metricKey": "custom_range",
                "label": "自定义区间",
                "cells": [
                    {
                        "key": bucket,
                        **_metric_cell(
                            float(grouped_custom_range.loc[bucket].sum()) if bucket in grouped_custom_range.index else 0.0,
                            "volume",
                        ),
                    }
                    for bucket in SEGMENT_MATRIX_ORDER
                ],
            },
            {
                "metricKey": "custom_range_yoy",
                "label": "自定义区间 YoY",
                "cells": [
                    {
                        "key": bucket,
                        **_delta_payload(
                            float(grouped_custom_range.loc[bucket].sum()) if bucket in grouped_custom_range.index else 0.0,
                            float(grouped_prior_custom_range.loc[bucket].sum()) if bucket in grouped_prior_custom_range.index else 0.0,
                        ),
                    }
                    for bucket in SEGMENT_MATRIX_ORDER
                ],
            },
        ])

    trend_periods = _window_periods(available_periods, resolved_period, body_window_months)
    trend_items = []
    suv_segment_trend_items = []
    for period in trend_periods:
        column = _period_to_month_column(period)
        suv_volume = (
            float(working.loc[working["__segment_bucket"].isin(SUV_SEGMENT_SHARE_ORDER), column].sum())
            if column in working.columns
            else 0.0
        )
        sedan_volume = (
            float(working.loc[working["__segment_bucket"].isin(SEDAN_SEGMENT_SHARE_ORDER), column].sum())
            if column in working.columns
            else 0.0
        )
        total_volume = float(working[column].sum()) if column in working.columns else 0.0
        trend_items.append(
            {
                "period": period,
                "label": _short_period_label(period),
                "suvSharePct": _safe_share(suv_volume, total_volume),
                "sedanSharePct": _safe_share(sedan_volume, total_volume),
                "totalVolume": total_volume,
            }
        )
        suv_segment_trend_items.append(
            {
                "period": period,
                "label": _short_period_label(period),
                "totalVolume": total_volume,
                "segmentSharePct": {
                    bucket: _safe_share(
                        float(working.loc[working["__segment_bucket"] == bucket, column].sum()) if column in working.columns else 0.0,
                        total_volume,
                    )
                    for bucket in SUV_SEGMENT_SHARE_ORDER
                },
            }
        )

    latest = trend_items[-1] if trend_items else {"suvSharePct": 0.0, "sedanSharePct": 0.0, "totalVolume": 0.0}
    leading_body = "SUV" if latest["suvSharePct"] >= latest["sedanSharePct"] else "Sedan"
    leading_share = latest["suvSharePct"] if leading_body == "SUV" else latest["sedanSharePct"]
    fastest_segment = "-"
    fastest_delta = {"display": "-"}
    yoy_row = next((row for row in matrix_rows if row["metricKey"] == "yoy"), None)
    if yoy_row and yoy_row["cells"]:
        ranked = sorted(
            yoy_row["cells"],
            key=lambda cell: cell["value"] if cell.get("value") is not None else float("-inf"),
            reverse=True,
        )
        if ranked:
            fastest_segment = ranked[0]["key"]
            fastest_delta = ranked[0]

    return {
        "summaryText": f"{leading_body}市占率达 {leading_share * 100:.1f}% ，市场容量 {latest['totalVolume']:,.0f} 台，{fastest_segment} 同比 {fastest_delta['display']}。",
        "matrix": {"columns": list(SEGMENT_MATRIX_ORDER), "rows": matrix_rows},
        "bodyShareTrend": {"items": trend_items},
        "suvSegmentShareTrend": {"items": suv_segment_trend_items},
        "channelMix": {
            "options": CHANNEL_MIX_OPTIONS,
            "month": _build_channel_mix_window(
                working,
                [current_column],
                title=f"{_short_period_label(resolved_period)} Overall Channel Mix",
            ),
            "ytd": _build_channel_mix_window(
                working,
                current_ytd_columns,
                title=f"YTD {_short_period_label(resolved_period)} Overall Channel Mix",
            ),
            "rolling12": _build_channel_mix_window(
                working,
                current_rolling12_columns,
                title=f"L12M {_short_period_label(resolved_period)} Overall Channel Mix",
            ),
            "customRange": _build_channel_mix_window(
                working,
                custom_range_columns,
                title="Custom Range Overall Channel Mix",
            ) if custom_range_periods else None,
        },
    }


def _build_total_ranking_items(
    frame: pd.DataFrame,
    current_columns: list[str],
    prior_columns: list[str],
    fuel_order: list[str],
    ranking_limit: int,
) -> list[dict[str, Any]]:
    current_total = _total_volume(frame, current_columns)
    grouped = frame.groupby("__model", dropna=False)
    items = []
    for model, group in grouped:
        current_volume = float(_series_sum(group, current_columns).sum())
        if current_volume <= 0:
            continue
        prior_volume = float(_series_sum(group, prior_columns).sum())
        fuel_mix = {
            fuel: float(_series_sum(group[group["__powertrain"] == fuel], current_columns).sum())
            for fuel in fuel_order
        }
        drive_mix = {
            drive_type: float(_series_sum(group[group["__drive_type"] == drive_type], current_columns).sum())
            for drive_type in ("2WD", "4WD", "OTHER")
        }
        registration_mix = _registration_mix_payload(group, current_columns)
        drive_share_pct = _safe_share(drive_mix["4WD"], current_volume)
        items.append(
            {
                "model": str(model),
                "volume": current_volume,
                "sharePct": _safe_share(current_volume, current_total),
                "shareDisplay": f"{_safe_share(current_volume, current_total) * 100:.1f}%",
                "yoy": _delta_payload(current_volume, prior_volume),
                "fuelMix": fuel_mix,
                "driveMix": drive_mix,
                "registrationMix": registration_mix,
                "driveSharePct": drive_share_pct,
                "driveShareDisplay": f"{drive_share_pct * 100:.1f}%",
            }
        )
    items.sort(
        key=lambda item: (-item["sharePct"], -item["volume"], item["model"]),
    )
    if not items:
        return []
    max_share = items[0]["sharePct"] or 1.0
    for rank, item in enumerate(items[: max(1, int(ranking_limit))], start=1):
        item["rank"] = rank
        item["barPct"] = _safe_share(item["sharePct"], max_share)
    return items[: max(1, int(ranking_limit))]


def _build_single_fuel_ranking_items(  # noqa: keep volume-based sort
    frame: pd.DataFrame,
    fuel_type: str,
    current_columns: list[str],
    prior_columns: list[str],
    segment_total: float,
    ranking_limit: int,
) -> list[dict[str, Any]]:
    filtered = frame[frame["__powertrain"] == fuel_type].copy()
    if filtered.empty:
        return []
    grouped = filtered.groupby("__model", dropna=False)
    items = []
    for model, group in grouped:
        current_volume = float(_series_sum(group, current_columns).sum())
        if current_volume <= 0:
            continue
        prior_volume = float(_series_sum(group, prior_columns).sum())
        
        current_4wd_volume = float(_series_sum(group[group["__drive_type"] == "4WD"], current_columns).sum())
        current_2wd_volume = float(_series_sum(group[group["__drive_type"] == "2WD"], current_columns).sum())
        current_other_volume = float(_series_sum(group[group["__drive_type"] == "OTHER"], current_columns).sum())
        registration_mix = _registration_mix_payload(group, current_columns)
        
        drive_share_pct = float(current_4wd_volume / current_volume) if current_volume > 0 else 0.0
        
        items.append(
            {
                "model": str(model),
                "volume": current_volume,
                "sharePct": _safe_share(current_volume, segment_total),
                "shareDisplay": f"{_safe_share(current_volume, segment_total) * 100:.1f}%",
                "yoy": _delta_payload(current_volume, prior_volume),
                "driveSharePct": drive_share_pct,
                "driveMix": {
                    "4WD": current_4wd_volume,
                    "2WD": current_2wd_volume,
                    "OTHER": current_other_volume,
                },
                "registrationMix": registration_mix,
            }
        )
    items.sort(key=lambda item: item["volume"], reverse=True)
    if not items:
        return []
    max_volume = items[0]["volume"] or 1.0
    for rank, item in enumerate(items[: max(1, int(ranking_limit))], start=1):
        item["rank"] = rank
        item["barPct"] = _safe_share(item["volume"], max_volume)
    return items[: max(1, int(ranking_limit))]


def _build_cross_tab_pct(
    frame: pd.DataFrame,
    *,
    index_col: str,
    column_col: str,
    index_values: list[str] | None = None,
    column_values: list[str] | None = None,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    if frame.empty or index_col not in frame.columns or column_col not in frame.columns:
        return []
    if "__sales" not in frame.columns:
        return []
    working = frame[frame["__sales"] > 0].copy()
    if working.empty:
        return []
    if index_values is None:
        index_totals = working.groupby(index_col)["__sales"].sum().sort_values(ascending=False)
        index_values = [str(v) for v in index_totals.head(top_n).index.tolist()]
    if column_values is None:
        col_totals = working.groupby(column_col)["__sales"].sum().sort_values(ascending=False)
        column_values = [str(v) for v in col_totals.head(top_n).index.tolist()]
    grouped = (
        working.groupby([index_col, column_col], dropna=False)["__sales"]
        .sum()
        .reset_index()
    )
    result: list[dict[str, Any]] = []
    for idx_val in index_values:
        idx_group = grouped[grouped[index_col].astype(str).str.strip() == str(idx_val).strip()]
        if idx_group.empty:
            continue
        total = float(idx_group["__sales"].sum())
        if total <= 0:
            continue
        row: dict[str, Any] = {"_index": str(idx_val).strip(), "_total": int(round(total))}
        for col_val in column_values:
            col_label = str(col_val).strip()
            col_sales = float(
                idx_group[idx_group[column_col].astype(str).str.strip() == col_label][
                    "__sales"
                ].sum()
            )
            row[col_label + "_pct"] = round(col_sales / total * 100, 1)
        result.append(row)
    result.sort(key=lambda r: r["_total"], reverse=True)
    return result[:top_n]


def build_causal_cross_tabs(
    country: str,
    target_period: str | None = None,
    fuel_types: list[str] | None = None,
) -> dict[str, Any]:
    columns = _get_columns()
    available_periods = _available_periods(columns)
    resolved_period = _resolve_period(target_period, available_periods)
    if fuel_types is None:
        fuel_types = list(DEFAULT_FUEL_TYPES)
    sales_column = _period_to_month_column(resolved_period)
    if sales_column not in columns.month_columns:
        return {"availableDimensions": [], "_note": "Sales period unavailable"}
    country_options = _country_options(repo.current_dataset_token())
    selected_country = _normalize_country_lookup(country, country_options)
    selected_columns = [
        columns.country_value,
        columns.segment,
        columns.powertrain,
    ]
    for extra_col in (columns.drive_type, columns.registration_type, columns.origin):
        if extra_col and extra_col not in selected_columns:
            selected_columns.append(extra_col)
    selected_columns.append(sales_column)
    dataset = repo._open_dataset()
    filter_expression = repo._build_filter_expression(
        {columns.country_value: [selected_country["value"]]}
    )
    table = dataset.to_table(columns=selected_columns, filter=filter_expression)
    frame = table.to_pandas()
    frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
    frame["__segment_raw"] = frame[columns.segment].astype(str).str.strip()
    frame["__sales"] = pd.to_numeric(frame[sales_column], errors="coerce").fillna(0.0)
    frame = frame[frame["__sales"] > 0]
    frame = frame[frame["__powertrain"].isin(fuel_types)].copy()
    available_dimensions: list[str] = []
    result: dict[str, Any] = {"availableDimensions": available_dimensions}

    has_drive = bool(columns.drive_type and columns.drive_type in frame.columns)
    has_reg = bool(columns.registration_type and columns.registration_type in frame.columns)
    has_origin = bool(columns.origin and columns.origin in frame.columns)

    if has_drive:
        frame["__drive_type"] = frame[columns.drive_type].map(_normalize_drive_type)
        available_dimensions.append("drive_type")
        result["driveByFuel"] = _build_cross_tab_pct(
            frame,
            index_col="__powertrain",
            column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"],
            top_n=8,
        )
        result["driveBySegment"] = _build_cross_tab_pct(
            frame,
            index_col="__segment_raw",
            column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"],
            top_n=10,
        )

    if has_reg:
        frame["__registration_type"] = frame[columns.registration_type].map(
            _normalize_registration_type
        )
        available_dimensions.append("registration_type")
        result["registrationByFuel"] = _build_cross_tab_pct(
            frame,
            index_col="__powertrain",
            column_col="__registration_type",
            column_values=["Business", "Private", "Other"],
            top_n=8,
        )
        result["registrationBySegment"] = _build_cross_tab_pct(
            frame,
            index_col="__segment_raw",
            column_col="__registration_type",
            column_values=["Business", "Private", "Other"],
            top_n=10,
        )

    result["segmentByFuel"] = _build_cross_tab_pct(
        frame,
        index_col="__segment_raw",
        column_col="__powertrain",
        top_n=10,
    )

    result["fuelBySegment"] = _build_cross_tab_pct(
        frame,
        index_col="__powertrain",
        column_col="__segment_raw",
        top_n=8,
    )

    if has_origin and has_drive:
        frame["__origin"] = frame[columns.origin].map(_normalize_origin)
        result["driveByOrigin"] = _build_cross_tab_pct(
            frame,
            index_col="__origin",
            column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"],
            top_n=6,
        )

    if has_origin and has_reg:
        result["registrationByOrigin"] = _build_cross_tab_pct(
            frame,
            index_col="__origin",
            column_col="__registration_type",
            column_values=["Business", "Private", "Other"],
            top_n=6,
        )

    return result


def _safe_build_cross_tabs(
    frame: pd.DataFrame,
    *,
    columns: ColumnMap,
    selected_fuels: list[str],
    sales_column: str,
) -> dict[str, Any]:
    try:
        return _build_cross_tabs_from_frame(
            frame,
            columns=columns,
            selected_fuels=selected_fuels,
            sales_column=sales_column,
        )
    except Exception:
        import logging
        logging.getLogger(__name__).warning(
            "Cross-tab builder failed, returning empty", exc_info=True,
        )
        return {"availableDimensions": []}


def _build_cross_tabs_from_frame(
    frame: pd.DataFrame,
    *,
    columns: ColumnMap,
    selected_fuels: list[str],
    sales_column: str,
) -> dict[str, Any]:
    working = frame.copy()
    working["__sales"] = pd.to_numeric(working[sales_column], errors="coerce").fillna(0.0)
    working = working[working["__sales"] > 0]
    available_dimensions: list[str] = []
    result: dict[str, Any] = {"availableDimensions": available_dimensions}

    has_drive = bool(columns.drive_type and columns.drive_type in working.columns)
    has_reg = bool(columns.registration_type and columns.registration_type in working.columns)
    has_origin = bool(columns.origin and columns.origin in working.columns)

    if has_drive:
        available_dimensions.append("drive_type")
        result["driveByFuel"] = _build_cross_tab_pct(
            working, index_col="__powertrain", column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"], top_n=8,
        )
        result["driveBySegment"] = _build_cross_tab_pct(
            working, index_col="__segment_raw", column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"], top_n=10,
        )

    if has_reg:
        available_dimensions.append("registration_type")
        result["registrationByFuel"] = _build_cross_tab_pct(
            working, index_col="__powertrain", column_col="__registration_type",
            column_values=["Business", "Private", "Other"], top_n=8,
        )
        result["registrationBySegment"] = _build_cross_tab_pct(
            working, index_col="__segment_raw", column_col="__registration_type",
            column_values=["Business", "Private", "Other"], top_n=10,
        )

    result["segmentByFuel"] = _build_cross_tab_pct(
        working, index_col="__segment_raw", column_col="__powertrain", top_n=10,
    )
    result["fuelBySegment"] = _build_cross_tab_pct(
        working, index_col="__powertrain", column_col="__segment_raw", top_n=8,
    )

    if has_origin and has_drive:
        result["driveByOrigin"] = _build_cross_tab_pct(
            working, index_col="__origin", column_col="__drive_type",
            column_values=["4WD", "2WD", "OTHER"], top_n=6,
        )
    if has_origin and has_reg:
        result["registrationByOrigin"] = _build_cross_tab_pct(
            working, index_col="__origin", column_col="__registration_type",
            column_values=["Business", "Private", "Other"], top_n=6,
        )

    return result


def _build_ytd_fuel_trend(
    frame: pd.DataFrame,
    fuel_order: list[str],
    resolved_period: str,
    available_periods: list[str],
) -> dict[str, Any]:
    target = pd.Period(resolved_period, freq="M")
    years = [target.year - 2, target.year - 1, target.year]
    items = []
    for year in years:
        ytd_periods = [
            period
            for period in available_periods
            if pd.Period(period, freq="M").year == year
            and pd.Period(period, freq="M").month <= target.month
        ]
        period_columns = [_period_to_month_column(period) for period in ytd_periods]
        total = _total_volume(frame, period_columns)
        label = f"{str(year)[2:4]},1-{target.month:02d}"
        fuel_mix = {
            fuel: float(_total_volume(frame[frame["__powertrain"] == fuel], period_columns))
            for fuel in fuel_order
        }
        items.append({"label": label, "totalVolume": total, "fuelMix": fuel_mix})
    return {"items": items}


def _build_rolling12_fuel_trend(
    frame: pd.DataFrame,
    fuel_order: list[str],
    resolved_period: str,
    available_periods: list[str],
) -> dict[str, Any]:
    target = pd.Period(resolved_period, freq="M")
    years = [target.year - 2, target.year - 1, target.year]
    items = []
    for year in years:
        end_period = f"{year}-{target.month:02d}"
        rolling_periods = _window_periods_if_present(available_periods, end_period, 12)
        period_columns = [_period_to_month_column(period) for period in rolling_periods]
        total = _total_volume(frame, period_columns)
        label = f"L12M {str(year)[2:4]}.{target.month:02d}"
        fuel_mix = {
            fuel: float(_total_volume(frame[frame["__powertrain"] == fuel], period_columns))
            for fuel in fuel_order
        }
        items.append({"label": label, "totalVolume": total, "fuelMix": fuel_mix})
    return {"items": items}


def _build_month_fuel_trend(
    frame: pd.DataFrame,
    fuel_order: list[str],
    resolved_period: str,
    available_periods: list[str],
) -> dict[str, Any]:
    target = pd.Period(resolved_period, freq="M")
    years = [target.year - 2, target.year - 1, target.year]
    items = []
    for year in years:
        period = f"{year}-{target.month:02d}"
        period_columns = [_period_to_month_column(period)] if period in available_periods else []
        total = _total_volume(frame, period_columns)
        label = f"{str(year)[2:4]}.{target.month:02d}"
        fuel_mix = {
            fuel: float(_total_volume(frame[frame["__powertrain"] == fuel], period_columns))
            for fuel in fuel_order
        }
        items.append({"label": label, "totalVolume": total, "fuelMix": fuel_mix})
    return {"items": items}


def _build_custom_range_fuel_trend(
    frame: pd.DataFrame,
    fuel_order: list[str],
    custom_range_periods: list[str],
) -> dict[str, Any]:
    items = []
    for period in custom_range_periods:
        period_columns = [_period_to_month_column(period)]
        total = _total_volume(frame, period_columns)
        fuel_mix = {
            fuel: float(_total_volume(frame[frame["__powertrain"] == fuel], period_columns))
            for fuel in fuel_order
        }
        items.append(
            {
                "label": _short_period_label(period),
                "totalVolume": total,
                "fuelMix": fuel_mix,
            }
        )
    return {"items": items}


def _empty_drilldown_payload(segment_value: str) -> dict[str, Any]:
    return {
        "segment": segment_value,
        "segmentLabel": _segment_display_label(segment_value),
        "title": f"{_segment_display_label(segment_value)} 车型",
        "summaryText": "当前筛选下没有该细分市场的可用数据。",
        "monthTotalRanking": {"title": "Monthly Total Model Ranking", "items": []},
        "rolling12TotalRanking": {"title": "Rolling 12M Total Model Ranking", "items": []},
        "totalRanking": {"title": "YTD Total Model Ranking", "items": []},
        "monthFuelTrend": {"items": []},
        "rolling12FuelTrend": {"items": []},
        "ytdFuelTrend": {"items": []},
        "fuelPanels": [],
    }


def precompute_model_rankings(
    frame: pd.DataFrame,
    current_month_columns: list[str],
    same_month_columns: list[str],
    current_ytd_columns: list[str],
    prior_ytd_columns: list[str],
    current_rolling12_columns: list[str],
    prior_rolling12_columns: list[str],
    custom_range_columns: list[str],
    prior_custom_range_columns: list[str],
    fuel_order: list[str],
) -> list[dict[str, Any]]:
    """Group by [segment, model] once and compute all window volumes."""
    grouped = frame.groupby(["__segment_raw", "__model"], dropna=False, sort=False)
    stats: list[dict[str, Any]] = []
    for (segment, model), group in grouped:
        month_vol = float(_series_sum(group, current_month_columns).sum())
        ytd_vol = float(_series_sum(group, current_ytd_columns).sum())
        rolling_vol = float(_series_sum(group, current_rolling12_columns).sum())
        if ytd_vol <= 0 and month_vol <= 0 and rolling_vol <= 0:
            continue
        stats.append({
            "segment": str(segment).strip(),
            "model": str(model).strip(),
            "monthVolume": month_vol,
            "monthPrior": float(_series_sum(group, same_month_columns).sum()),
            "ytdVolume": ytd_vol,
            "ytdPrior": float(_series_sum(group, prior_ytd_columns).sum()),
            "rollingVolume": rolling_vol,
            "rollingPrior": float(_series_sum(group, prior_rolling12_columns).sum()),
            "customVolume": float(_series_sum(group, custom_range_columns).sum()) if custom_range_columns else 0,
            "customPrior": float(_series_sum(group, prior_custom_range_columns).sum()) if prior_custom_range_columns else 0,
            "fuelMix": {
                fuel: float(_series_sum(group[group["__powertrain"] == fuel], current_ytd_columns).sum())
                for fuel in fuel_order
            },
            "driveMix": {
                dt: float(_series_sum(group[group["__drive_type"] == dt], current_ytd_columns).sum())
                for dt in ("2WD", "4WD", "OTHER")
            },
            "registrationMix": _registration_mix_payload(group, current_ytd_columns),
        })
    return stats


def model_stats_to_ranking(
    stats: list[dict[str, Any]],
    segment: str,
    vol_key: str,
    prior_key: str,
    fuel_order: list[str],
    ranking_limit: int,
) -> list[dict[str, Any]]:
    """Convert pre-computed model stats into ranked items for a single segment."""
    items = [s for s in stats if s["segment"] == segment]
    if not items:
        return []
    total = sum(s[vol_key] for s in items)
    if total <= 0:
        return []
    # Compute shares locally — avoid mutating shared stats dicts
    with_share = [(s, s[vol_key] / total) for s in items]
    with_share.sort(key=lambda x: (-x[1], -x[0][vol_key], x[0]["model"]))
    max_share = with_share[0][1] or 1.0
    result = []
    for rank, (s, share) in enumerate(with_share[:max(1, int(ranking_limit))], start=1):
        result.append({
            "model": s["model"],
            "volume": s[vol_key],
            "sharePct": share,
            "shareDisplay": f"{share * 100:.1f}%",
            "yoy": _delta_payload(s[vol_key], s[prior_key]),
            "fuelMix": s["fuelMix"],
            "driveMix": s["driveMix"],
            "registrationMix": s["registrationMix"],
            "driveSharePct": _safe_share(s["driveMix"].get("4WD", 0), s[vol_key]),
            "driveShareDisplay": f"{_safe_share(s['driveMix'].get('4WD', 0), s[vol_key]) * 100:.1f}%",
            "rank": rank,
            "barPct": _safe_share(share, max_share),
        })
    return result


def _build_all_drilldowns(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    same_month_last_year_period: str | None,
    segment_values: list[str],
    fuel_panels: tuple[str, ...],
    ranking_limit: int,
    custom_range_periods: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Compute drilldown payloads for multiple segments in a single model-level groupby pass."""
    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)

    current_month_columns = [_period_to_month_column(resolved_period)]
    same_month_columns = [_period_to_month_column(same_month_last_year_period)] if same_month_last_year_period else []
    current_ytd_columns = [_period_to_month_column(p) for p in _ytd_periods(available_periods, resolved_period)]
    prior_ytd_columns = [_period_to_month_column(p) for p in _ytd_periods(available_periods, _shift_period(resolved_period, -12))] if same_month_last_year_period else []
    current_rolling12_columns = [_period_to_month_column(p) for p in _window_periods_if_present(available_periods, resolved_period, 12)]
    prior_rolling12_columns = [_period_to_month_column(p) for p in _window_periods_if_present(available_periods, _shift_period(resolved_period, -12), 12)]
    custom_range_columns = [_period_to_month_column(p) for p in (custom_range_periods or [])]
    prior_custom_range_periods = _shifted_periods_if_present(custom_range_periods, available_periods, -12) if custom_range_periods else []
    prior_custom_range_columns = [_period_to_month_column(p) for p in prior_custom_range_periods]

    available_fuels = _available_fuel_types(frame)

    # ONE groupby pass over all segments
    all_stats = precompute_model_rankings(
        frame,
        current_month_columns, same_month_columns,
        current_ytd_columns, prior_ytd_columns,
        current_rolling12_columns, prior_rolling12_columns,
        custom_range_columns, prior_custom_range_columns,
        fuel_order=available_fuels,
    )

    result: dict[str, dict[str, Any]] = {}
    for seg in segment_values:
        seg_frame = frame[frame["__segment_raw"] == seg]
        if seg_frame.empty or not any(s["segment"] == seg for s in all_stats):
            result[seg] = _empty_drilldown_payload(seg)
            continue

        seg_stats = [s for s in all_stats if s["segment"] == seg]
        seg_fuels = _available_fuel_types(seg_frame)
        seg_total_ytd = sum(s["ytdVolume"] for s in seg_stats)
        seg_total_month = sum(s["monthVolume"] for s in seg_stats)
        seg_total_rolling = sum(s["rollingVolume"] for s in seg_stats)
        seg_total_custom = sum(s["customVolume"] for s in seg_stats)

        # Fuel panels: per-fuel rankings (each still needs a sub-groupby, but scoped to segment)
        fuel_panel_items = []
        for fuel_type in fuel_panels:
            fuel_frame = seg_frame[seg_frame["__powertrain"] == fuel_type]
            fuel_panel_items.append({
                "fuelType": fuel_type,
                "ytdTitle": f"{fuel_type} 1-{month_number}月累计",
                "rolling12Title": f"{fuel_type} 近12个月 · 截至 {_short_period_label(resolved_period)}",
                "monthTitle": f"{fuel_type} {_short_period_label(resolved_period)}",
                "ytdRanking": _build_single_fuel_ranking_items(
                    fuel_frame, fuel_type=fuel_type, current_columns=current_ytd_columns,
                    prior_columns=prior_ytd_columns, segment_total=seg_total_ytd, ranking_limit=ranking_limit,
                ),
                "rolling12Ranking": _build_single_fuel_ranking_items(
                    fuel_frame, fuel_type=fuel_type, current_columns=current_rolling12_columns,
                    prior_columns=prior_rolling12_columns, segment_total=seg_total_rolling, ranking_limit=ranking_limit,
                ),
                "monthRanking": _build_single_fuel_ranking_items(
                    fuel_frame, fuel_type=fuel_type, current_columns=current_month_columns,
                    prior_columns=same_month_columns, segment_total=seg_total_month, ranking_limit=ranking_limit,
                ),
            })

        rolling12_total = model_stats_to_ranking(all_stats, seg, "rollingVolume", "rollingPrior", seg_fuels, ranking_limit)
        headline_model = rolling12_total[0]["model"] if rolling12_total else "市场"
        headline_yoy = rolling12_total[0]["yoy"]["display"] if rolling12_total else "-"

        result[seg] = {
            "segment": seg,
            "segmentLabel": _segment_display_label(seg),
            "title": f"{_segment_display_label(seg)} 车型 {year_text}年1-{month_number}月",
            "summaryText": f"{headline_model} 目前领跑 {_segment_display_label(seg)}，近12个月同比 {headline_yoy}。",
            "monthTotalRanking": {
                "title": "Monthly Total Model Ranking",
                "items": model_stats_to_ranking(all_stats, seg, "monthVolume", "monthPrior", seg_fuels, ranking_limit),
            },
            "rolling12TotalRanking": {
                "title": "Rolling 12M Total Model Ranking",
                "items": rolling12_total,
            },
            "customRangeTotalRanking": {
                "title": "Custom Range Total Model Ranking",
                "items": model_stats_to_ranking(all_stats, seg, "customVolume", "customPrior", seg_fuels, ranking_limit),
            } if custom_range_periods else None,
            "totalRanking": {
                "title": "YTD Total Model Ranking",
                "items": model_stats_to_ranking(all_stats, seg, "ytdVolume", "ytdPrior", seg_fuels, ranking_limit),
            },
            "monthFuelTrend": _build_month_fuel_trend(seg_frame, seg_fuels, resolved_period, available_periods),
            "rolling12FuelTrend": _build_rolling12_fuel_trend(seg_frame, seg_fuels, resolved_period, available_periods),
            "ytdFuelTrend": _build_ytd_fuel_trend(seg_frame, seg_fuels, resolved_period, available_periods),
            "customRangeFuelTrend": {
                **_build_custom_range_fuel_trend(seg_frame, seg_fuels, custom_range_periods),
            } if custom_range_periods else None,
            "fuelPanels": fuel_panel_items,
        }
    return result


def query_market_scan_deck(
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    trend_window_months: int,
    origin_window_months: int,
    body_window_months: int,
    ranking_limit: int,
    drilldown_segment: str | None,
) -> dict[str, Any]:
    ranking_limit = max(MIN_MARKET_SCAN_RANKING_LIMIT, int(ranking_limit))
    time_range_key = ""
    if time_range:
        time_range_key = f"{time_range.get('start', '')}:{time_range.get('end', '')}"
    cache_key = f"{country}|{target_period}|{time_range_key}|{','.join(sorted(fuel_types))}|{trend_window_months}|{origin_window_months}|{body_window_months}|{ranking_limit}|{drilldown_segment}"
    now = time.monotonic()
    dataset_token = repo.current_dataset_token()
    cached = _deck_cache.get(cache_key)
    if cached is not None:
        cached_at, cached_token, cached_result = cached
        if cached_token == dataset_token and (now - cached_at) < _DECK_CACHE_TTL_SECONDS:
            return cached_result

    result = _query_market_scan_deck_impl(
        country, target_period, time_range, fuel_types,
        trend_window_months, origin_window_months, body_window_months,
        ranking_limit, drilldown_segment,
    )

    with _deck_cache_lock:
        _deck_cache[cache_key] = (now, dataset_token, result)
        if len(_deck_cache) > 32:
            oldest_key = min(_deck_cache, key=lambda k: _deck_cache[k][0])
            _deck_cache.pop(oldest_key, None)

    return result


def query_positioning_pricing_deck(
    *,
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    sales_mode: str,
    top_n: int,
    msrp_min: float | None,
    msrp_max: float | None,
    length_min: float | None,
    length_max: float | None,
    price_band_size: int | None,
) -> dict[str, Any]:
    return _query_positioning_pricing_deck_impl(
        country=country,
        target_period=target_period,
        time_range=time_range,
        fuel_types=fuel_types,
        sales_mode=sales_mode,
        top_n=top_n,
        msrp_min=msrp_min,
        msrp_max=msrp_max,
        length_min=length_min,
        length_max=length_max,
        price_band_size=price_band_size,
    )


def _query_positioning_pricing_deck_impl(
    *,
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    sales_mode: str,
    top_n: int,
    msrp_min: float | None,
    msrp_max: float | None,
    length_min: float | None,
    length_max: float | None,
    price_band_size: int | None,
) -> dict[str, Any]:
    columns = _get_columns()
    if not columns.length or not columns.msrp:
        raise RuntimeError("Positioning pricing columns are incomplete")

    available_periods = _available_periods(columns)
    resolved_period = _resolve_period(target_period, available_periods)
    custom_periods = _normalize_period_range(available_periods, time_range, resolved_period)
    selected_sales_mode = _normalize_positioning_sales_mode(sales_mode)
    sales_periods, sales_mode_label, sales_metric_label = _resolve_positioning_sales_window(
        available_periods,
        resolved_period,
        selected_sales_mode,
        custom_periods,
    )
    sales_columns = [_period_to_month_column(period) for period in sales_periods]
    sales_column = "__positioning_sales"

    country_options = _country_options(repo.current_dataset_token())
    selected_country = _normalize_country_lookup(country, country_options)

    selected_columns = [
        columns.country_value,
        columns.make,
        columns.model,
        columns.segment,
        columns.powertrain,
        columns.length,
        columns.msrp,
        *sales_columns,
    ]
    if columns.trim and columns.trim not in selected_columns:
        selected_columns.append(columns.trim)
    if columns.country_label and columns.country_label not in selected_columns:
        selected_columns.append(columns.country_label)

    dataset = repo._open_dataset()
    filter_expression = repo._build_filter_expression({columns.country_value: [selected_country["value"]]})
    table = dataset.to_table(columns=selected_columns, filter=filter_expression)
    frame = table.to_pandas()
    frame = _ensure_numeric_columns(frame, [*sales_columns, columns.length, columns.msrp])
    frame["__brand"] = frame[columns.make].astype(str).str.strip()
    frame["__model"] = frame[columns.model].astype(str).str.strip()
    frame["__segment_raw"] = frame[columns.segment].astype(str).str.strip()
    frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
    frame["__trim"] = (
        frame[columns.trim].astype(str).str.strip()
        if columns.trim and columns.trim in frame.columns
        else ""
    )
    frame["__length"] = pd.to_numeric(frame[columns.length], errors="coerce").fillna(0.0)
    frame["__msrp"] = pd.to_numeric(frame[columns.msrp], errors="coerce").fillna(0.0)
    frame[sales_column] = _series_sum(frame, sales_columns)
    frame = frame[
        (frame["__brand"] != "")
        & (frame["__model"] != "")
        & (frame["__segment_raw"] != "")
        & (frame["__powertrain"] != "OTHER")
        & (frame["__length"] > 0)
        & (frame["__msrp"] > 0)
    ].copy()

    current_price_candidates, jato_link_candidates, price_overlay_meta = _load_positioning_overlay_candidates(
        selected_country,
        frame,
    )
    frame, overlay_result_meta = _apply_positioning_current_price_overlay(
        frame,
        current_price_candidates,
        jato_link_candidates,
    )
    price_overlay_meta = {
        "sourceMode": price_overlay_meta.get("mode"),
        **price_overlay_meta,
        **overlay_result_meta,
    }

    available_fuels = [
        fuel for fuel in POSITIONING_FUEL_ORDER
        if fuel in _available_fuel_types(frame)
    ]
    selected_fuels = _normalize_selected_fuels(fuel_types, available_fuels)
    selected_fuels = [fuel for fuel in POSITIONING_FUEL_ORDER if fuel in selected_fuels]
    filtered_frame = frame[frame["__powertrain"].isin(selected_fuels)].copy()

    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)
    if selected_sales_mode == "rolling12":
        page_title = f"{selected_country['label']} 截至{int(year_text)}年{month_number}月近12个月定位定价"
        sales_metric_detail = "近12个月销量"
    elif selected_sales_mode == "ytd":
        page_title = f"{selected_country['label']} {int(year_text)}年1-{month_number}月YTD定位定价"
        sales_metric_detail = "YTD销量"
    else:
        page_title = f"{selected_country['label']} {int(year_text)}年{month_number}月定位定价"
        sales_metric_detail = "当月销量"
    if custom_periods:
        page_title = f"{selected_country['label']} {custom_periods[0]} ~ {custom_periods[-1]} 自定义区间定位定价"
        sales_metric_detail = "自定义区间累计销量"
    metadata = {
        "protocolVersion": "positioning-pricing/v1",
        "requestedPeriod": target_period,
        "resolvedPeriod": resolved_period,
        "latestPeriod": available_periods[-1],
        "selectedCountry": selected_country["value"],
        "selectedCountryLabel": selected_country["label"],
        "selectedFuelTypes": selected_fuels,
            "selectedSalesMode": selected_sales_mode,
            "selectedTimeRange": {
                "start": custom_periods[0],
                "end": custom_periods[-1],
            } if custom_periods else None,
            "customRangeActive": bool(custom_periods),
            "selectedTopN": int(top_n),
        "availableSalesModes": [
            {"value": "month", "label": "当月"},
            {"value": "ytd", "label": "YTD"},
            {"value": "rolling12", "label": "近12个月"},
        ],
        "availableCountries": country_options,
        "availablePeriods": [{"value": period, "label": _short_period_label(period)} for period in available_periods],
        "availableFuelTypes": available_fuels,
        "priceOverlay": price_overlay_meta,
        "labels": {
            "pageTitle": page_title,
            "currentMonthShort": _short_period_label(resolved_period),
            "salesModeLabel": sales_mode_label,
        },
    }

    return {
        "metadata": metadata,
        "pages": {
            "overview": _build_positioning_page_payload(
                filtered_frame,
                page_key="overview",
                title="市场总览",
                subtitle="全市场价格带与动力定位",
                sales_column=sales_column,
                sales_metric_label=sales_metric_label,
                sales_metric_detail=sales_metric_detail,
                selected_fuels=selected_fuels,
                top_n=top_n,
                msrp_min=msrp_min,
                msrp_max=msrp_max,
                price_band_size=price_band_size,
                length_min=length_min,
                length_max=length_max,
            ),
            "suvAll": _build_positioning_page_payload(
                filtered_frame,
                page_key="suvAll",
                title="全 SUV",
                subtitle="全 SUV 价格带与动力定位",
                sales_column=sales_column,
                sales_metric_label=sales_metric_label,
                sales_metric_detail=sales_metric_detail,
                selected_fuels=selected_fuels,
                top_n=top_n,
                msrp_min=msrp_min,
                msrp_max=msrp_max,
                price_band_size=price_band_size,
                length_min=length_min,
                length_max=length_max,
            ),
            "suvA0": _build_positioning_page_payload(
                filtered_frame,
                page_key="suvA0",
                title="SUV-A0",
                subtitle="SUV A0 价格带与动力定位",
                sales_column=sales_column,
                sales_metric_label=sales_metric_label,
                sales_metric_detail=sales_metric_detail,
                selected_fuels=selected_fuels,
                top_n=top_n,
                msrp_min=msrp_min,
                msrp_max=msrp_max,
                price_band_size=price_band_size,
                length_min=length_min,
                length_max=length_max,
            ),
            "suvA": _build_positioning_page_payload(
                filtered_frame,
                page_key="suvA",
                title="SUV-A",
                subtitle="SUV A 价格带与动力定位",
                sales_column=sales_column,
                sales_metric_label=sales_metric_label,
                sales_metric_detail=sales_metric_detail,
                selected_fuels=selected_fuels,
                top_n=top_n,
                msrp_min=msrp_min,
                msrp_max=msrp_max,
                price_band_size=price_band_size,
                length_min=length_min,
                length_max=length_max,
            ),
            "suvBPlus": _build_positioning_page_payload(
                filtered_frame,
                page_key="suvBPlus",
                title="SUV-B+",
                subtitle="SUV B+ 价格带与动力定位",
                sales_column=sales_column,
                sales_metric_label=sales_metric_label,
                sales_metric_detail=sales_metric_detail,
                selected_fuels=selected_fuels,
                top_n=top_n,
                msrp_min=msrp_min,
                msrp_max=msrp_max,
                price_band_size=price_band_size,
                length_min=length_min,
                length_max=length_max,
            ),
        },
    }


def query_version_comparison_deck(
    *,
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    sales_mode: str,
    comparison_mode: str = "same_segment",
    segment: str | None,
    models: list[str],
    msrp_min: float | None,
    msrp_max: float | None,
    price_band_size: int | None,
    body_type: str | None = None,
    drive_types: list[str] | None = None,
    segments: list[str] | None = None,
    length_min: float | None = None,
    length_max: float | None = None,
) -> dict[str, Any]:
    return _query_version_comparison_deck_impl(
        country=country,
        target_period=target_period,
        time_range=time_range,
        fuel_types=fuel_types,
        sales_mode=sales_mode,
        comparison_mode=comparison_mode,
        segment=segment,
        models=models,
        msrp_min=msrp_min,
        msrp_max=msrp_max,
        price_band_size=price_band_size,
        body_type=body_type,
        drive_types=drive_types,
        segments=segments,
        length_min=length_min,
        length_max=length_max,
    )


def _query_version_comparison_deck_impl(
    *,
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    sales_mode: str,
    comparison_mode: str = "same_segment",
    segment: str | None,
    models: list[str],
    msrp_min: float | None,
    msrp_max: float | None,
    price_band_size: int | None,
    body_type: str | None = None,
    drive_types: list[str] | None = None,
    segments: list[str] | None = None,
    length_min: float | None = None,
    length_max: float | None = None,
) -> dict[str, Any]:
    columns = _get_columns()
    if not columns.length or not columns.msrp or not columns.version:
        raise RuntimeError("Version comparison columns are incomplete")

    available_periods = _available_periods(columns)
    resolved_period = _resolve_period(target_period, available_periods)
    custom_periods = _normalize_period_range(available_periods, time_range, resolved_period)
    selected_sales_mode = _normalize_positioning_sales_mode(sales_mode)
    sales_periods, sales_mode_label, sales_metric_label = _resolve_positioning_sales_window(
        available_periods,
        resolved_period,
        selected_sales_mode,
        custom_periods,
    )
    sales_columns = [_period_to_month_column(period) for period in sales_periods]
    sales_column = "__comparison_sales"

    country_options = _country_options(repo.current_dataset_token())
    selected_country = _normalize_country_lookup(country, country_options)

    selected_columns = [
        columns.country_value,
        columns.make,
        columns.model,
        columns.version,
        columns.segment,
        columns.powertrain,
        columns.length,
        columns.msrp,
        *sales_columns,
    ]
    if columns.trim and columns.trim not in selected_columns:
        selected_columns.append(columns.trim)
    if columns.country_label and columns.country_label not in selected_columns:
        selected_columns.append(columns.country_label)
    if columns.drive_type and columns.drive_type not in selected_columns:
        selected_columns.append(columns.drive_type)
    if columns.body_type and columns.body_type not in selected_columns:
        selected_columns.append(columns.body_type)
    selected_columns = list(dict.fromkeys(selected_columns))

    dataset = repo._open_dataset()
    filter_expression = repo._build_filter_expression({columns.country_value: [selected_country["value"]]})
    table = dataset.to_table(columns=selected_columns, filter=filter_expression)
    frame = table.to_pandas()
    numeric_columns = [*sales_columns, columns.length, columns.msrp]
    frame = _ensure_numeric_columns(frame, numeric_columns)
    frame["__brand"] = frame[columns.make].astype(str).str.strip()
    frame["__model"] = frame[columns.model].astype(str).str.strip()
    frame["__version"] = frame[columns.version].astype(str).str.strip()
    frame["__trim"] = frame[columns.trim].astype(str).str.strip() if columns.trim and columns.trim in frame.columns else frame["__version"]
    frame["__segment_raw"] = frame[columns.segment].astype(str).str.strip()
    frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
    frame["__length"] = pd.to_numeric(frame[columns.length], errors="coerce").fillna(0.0)
    frame["__msrp"] = pd.to_numeric(frame[columns.msrp], errors="coerce").fillna(0.0)
    frame["__drive_type"] = frame[columns.drive_type].astype(str).str.strip() if columns.drive_type and columns.drive_type in frame.columns else ""
    frame["__body_type"] = frame[columns.body_type].astype(str).str.strip() if columns.body_type and columns.body_type in frame.columns else ""
    frame[sales_column] = _series_sum(frame, sales_columns)
    frame = frame[
        (frame["__brand"] != "")
        & (frame["__model"] != "")
        & (frame["__version"] != "")
        & (frame["__segment_raw"] != "")
        & (frame["__powertrain"] != "OTHER")
        & (frame["__length"] > 0)
        & (frame["__msrp"] > 0)
        & (frame[sales_column] > 0)
    ].copy()

    available_fuels = [
        fuel for fuel in POSITIONING_FUEL_ORDER
        if fuel in _available_fuel_types(frame)
    ]
    selected_fuels = _normalize_selected_fuels(fuel_types, available_fuels)
    selected_fuels = [fuel for fuel in POSITIONING_FUEL_ORDER if fuel in selected_fuels]
    fuel_frame = frame[frame["__powertrain"].isin(selected_fuels)].copy()

    # --- Comparison mode logic ---
    if comparison_mode == "same_segment":
        # Segment = hard single filter
        selected_segment, available_segments = _resolve_version_comparison_segment(
            fuel_frame, segment, sales_column=sales_column,
        )
        segment_frame = fuel_frame[fuel_frame["__segment_raw"] == selected_segment].copy() if selected_segment else fuel_frame.iloc[0:0].copy()
        selected_models, _ = _resolve_version_comparison_models(
            segment_frame, models, sales_column=sales_column,
        )
        comparison_frame = segment_frame[segment_frame["__model"].isin(selected_models)].copy() if selected_models else segment_frame.iloc[0:0].copy()
        candidate_model_options = _build_all_model_options(segment_frame, sales_column)
    else:  # free_comparison
        # Candidate pool: optional filters (body, length, msrp, drive, segments)
        candidate_frame = fuel_frame.copy()
        if body_type and body_type.strip():
            candidate_frame = candidate_frame[candidate_frame["__body_type"] == body_type.strip()]
        if length_min is not None:
            candidate_frame = candidate_frame[candidate_frame["__length"] >= float(length_min)]
        if length_max is not None:
            candidate_frame = candidate_frame[candidate_frame["__length"] <= float(length_max)]
        if msrp_min is not None:
            candidate_frame = candidate_frame[candidate_frame["__msrp"] >= float(msrp_min)]
        if msrp_max is not None:
            candidate_frame = candidate_frame[candidate_frame["__msrp"] <= float(msrp_max)]
        if drive_types:
            candidate_frame = candidate_frame[candidate_frame["__drive_type"].isin(drive_types)]
        if segments:
            candidate_frame = candidate_frame[candidate_frame["__segment_raw"].isin(segments)]
        available_segments = _build_model_option_list(fuel_frame, "__segment_raw", sales_column)
        selected_segment = ""
        # Selected basket: explicit models from full frame, default top 3 from candidate pool
        if models:
            selected_models, _ = _resolve_version_comparison_models(fuel_frame, models, sales_column=sales_column)
        else:
            selected_models, _ = _resolve_version_comparison_models(candidate_frame, [], sales_column=sales_column)
        # Candidate pool = models within the filtered candidate frame
        candidate_model_options = _build_all_model_options(candidate_frame, sales_column)
        comparison_frame = fuel_frame[fuel_frame["__model"].isin(selected_models)].copy() if selected_models else fuel_frame.iloc[0:0].copy()

    # Build enhanced model options with metadata
    def _build_enhanced_model_options(model_list: list[str]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        model_agg: dict[str, dict[str, Any]] = {}
        for model_name in model_list:
            model_rows = fuel_frame[fuel_frame["__model"] == model_name]
            if model_rows.empty:
                continue
            segments = model_rows["__segment_raw"].dropna().unique()
            powertrains = model_rows["__powertrain"].dropna().unique()
            body_types = model_rows["__body_type"].dropna().unique() if columns.body_type else []
            drive_types_list = model_rows["__drive_type"].dropna().unique() if columns.drive_type else []
            lengths = model_rows[model_rows["__length"] > 0]["__length"]
            msrps = model_rows[model_rows["__msrp"] > 0]["__msrp"]
            model_agg[model_name] = {
                "value": model_name,
                "label": model_name,
                "segment": str(segments[0]) if len(segments) > 0 else "",
                "powertrain": " / ".join(sorted(set(str(p) for p in powertrains))) if len(powertrains) > 0 else "",
                "bodyType": str(body_types[0]) if len(body_types) > 0 else "",
                "driveType": " / ".join(sorted(set(str(d) for d in drive_types_list))) if len(drive_types_list) > 0 else "",
                "lengthMm": float(lengths.median()) if len(lengths) > 0 else 0,
                "msrpMedian": float(msrps.median()) if len(msrps) > 0 else 0,
            }
        result = [model_agg[m] for m in model_list if m in model_agg]
        return result

    # Enhanced availableModels with metadata
    enhanced_available_models = _build_enhanced_model_options(
        [opt["value"] for opt in candidate_model_options]
    )
    # Detect mixed segment
    selected_model_details = _build_enhanced_model_options(selected_models)
    selected_segments_set = {m["segment"] for m in selected_model_details if m["segment"]}
    is_mixed_segment = len(selected_segments_set) > 1

    # Suggested length range: segment-based in same_segment, full range in free_comparison
    if comparison_mode == "same_segment" and selected_segment:
        seg_lengths = fuel_frame[fuel_frame["__segment_raw"] == selected_segment]["__length"]
        seg_lengths = seg_lengths[(seg_lengths > 0) & (seg_lengths < 6000)]
        if len(seg_lengths) > 0:
            suggested_length_min = float(seg_lengths.quantile(0.05))
            suggested_length_max = float(seg_lengths.quantile(0.95))
        else:
            suggested_length_min, suggested_length_max = None, None
    else:
        all_lengths = fuel_frame["__length"]
        all_lengths = all_lengths[(all_lengths > 0) & (all_lengths < 6000)]
        if len(all_lengths) > 0:
            suggested_length_min = float(all_lengths.min())
            suggested_length_max = float(all_lengths.max())
        else:
            suggested_length_min, suggested_length_max = None, None

    # Round to nearest 50
    if suggested_length_min is not None:
        suggested_length_min = round(suggested_length_min / 50) * 50
    if suggested_length_max is not None:
        suggested_length_max = round(suggested_length_max / 50) * 50

    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)
    if comparison_mode == "free_comparison":
        page_title = f"{selected_country['label']} 自由对比 {int(year_text)}年{month_number}月版型对比"
        sales_metric_detail = "当月销量"
    elif selected_sales_mode == "rolling12":
        page_title = f"{selected_country['label']} {selected_segment or 'Segment'} 截至{int(year_text)}年{month_number}月近12个月版型对比"
        sales_metric_detail = "近12个月销量"
    elif selected_sales_mode == "ytd":
        page_title = f"{selected_country['label']} {selected_segment or 'Segment'} {int(year_text)}年1-{month_number}月YTD版型对比"
        sales_metric_detail = "YTD销量"
    else:
        page_title = f"{selected_country['label']} {selected_segment or 'Segment'} {int(year_text)}年{month_number}月版型对比"
        sales_metric_detail = "当月销量"
    if custom_periods:
        segment_label = selected_segment or "多Segment"
        page_title = f"{selected_country['label']} {segment_label} {custom_periods[0]} ~ {custom_periods[-1]} 自定义区间版型对比"
        sales_metric_detail = "自定义区间累计销量"

    return {
        "metadata": {
            "protocolVersion": "version-comparison/v2",
            "requestedPeriod": target_period,
            "resolvedPeriod": resolved_period,
            "latestPeriod": available_periods[-1],
            "selectedCountry": selected_country["value"],
            "selectedCountryLabel": selected_country["label"],
            "selectedFuelTypes": selected_fuels,
            "selectedSalesMode": selected_sales_mode,
            "selectedTimeRange": {
                "start": custom_periods[0],
                "end": custom_periods[-1],
            } if custom_periods else None,
            "customRangeActive": bool(custom_periods),
            "comparisonMode": comparison_mode,
            "selectedSegment": selected_segment,
            "selectedModels": selected_models,
            "isMixedSegment": is_mixed_segment,
            "availableSalesModes": [
                {"value": "month", "label": "当月"},
                {"value": "ytd", "label": "YTD"},
                {"value": "rolling12", "label": "近12个月"},
            ],
            "availableCountries": country_options,
            "availablePeriods": [{"value": period, "label": _short_period_label(period)} for period in available_periods],
            "availableFuelTypes": available_fuels,
            "availableSegments": available_segments,
            "availableModels": enhanced_available_models,
            "availableBodyTypes": _available_body_types(fuel_frame) if columns.body_type else [],
            "availableDriveTypes": _available_drive_types(fuel_frame) if columns.drive_type else [],
            "suggestedLengthMin": suggested_length_min,
            "suggestedLengthMax": suggested_length_max,
            "labels": {
                "pageTitle": page_title,
                "currentMonthShort": _short_period_label(resolved_period),
                "salesModeLabel": sales_mode_label,
            },
        },
        "page": _build_version_comparison_page_payload(
            comparison_frame,
            title=selected_segment or "版型对比",
            subtitle="Model 版型对比与价格带",
            sales_column=sales_column,
            sales_metric_label=sales_metric_label,
            sales_metric_detail=sales_metric_detail,
            selected_fuels=selected_fuels,
            msrp_min=msrp_min,
            msrp_max=msrp_max,
            price_band_size=price_band_size,
        ),
    }


def _query_market_scan_deck_impl(
    country: str | None,
    target_period: str | None,
    time_range: dict[str, str] | None,
    fuel_types: list[str],
    trend_window_months: int,
    origin_window_months: int,
    body_window_months: int,
    ranking_limit: int,
    drilldown_segment: str | None,
) -> dict[str, Any]:
    columns = _get_columns()
    available_periods = _available_periods(columns)
    resolved_period = _resolve_period(target_period, available_periods)
    custom_periods = _normalize_period_range(available_periods, time_range, resolved_period)
    same_month_last_year_period = _shift_period(resolved_period, -12)
    if same_month_last_year_period not in available_periods:
        same_month_last_year_period = None
    prior_period = _shift_period(resolved_period, -1)
    if prior_period not in available_periods:
        prior_period = None

    country_options = _country_options(repo.current_dataset_token())
    selected_country = _normalize_country_lookup(country, country_options)

    selected_columns = [
        columns.country_value,
        columns.make,
        columns.model,
        columns.segment,
        columns.powertrain,
        *columns.month_columns,
    ]
    if columns.country_label and columns.country_label not in selected_columns:
        selected_columns.append(columns.country_label)
    if columns.origin and columns.origin not in selected_columns:
        selected_columns.append(columns.origin)
    if columns.drive_type and columns.drive_type not in selected_columns:
        selected_columns.append(columns.drive_type)
    if columns.registration_type and columns.registration_type not in selected_columns:
        selected_columns.append(columns.registration_type)

    dataset = repo._open_dataset()
    filter_expression = repo._build_filter_expression({columns.country_value: [selected_country["value"]]})
    table = dataset.to_table(columns=selected_columns, filter=filter_expression)
    frame = table.to_pandas()
    frame = _ensure_numeric_columns(frame, list(columns.month_columns))
    frame["__brand"] = frame[columns.make].astype(str).str.strip()
    frame["__model"] = frame[columns.model].astype(str).str.strip()
    frame["__segment_raw"] = frame[columns.segment].astype(str).str.strip()
    frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
    frame["__origin"] = frame[columns.origin].map(_normalize_origin) if columns.origin and columns.origin in frame.columns else "其他"
    frame["__drive_type"] = frame[columns.drive_type].map(_normalize_drive_type) if columns.drive_type and columns.drive_type in frame.columns else "OTHER"
    frame["__registration_type"] = (
        frame[columns.registration_type].map(_normalize_registration_type)
        if columns.registration_type and columns.registration_type in frame.columns else
        "Other"
    )

    available_fuels = _available_fuel_types(frame)
    selected_fuels = _normalize_selected_fuels(fuel_types, available_fuels)
    filtered_frame = frame[frame["__powertrain"].isin(selected_fuels)].copy()

    available_segments = sorted(
        {segment for segment in filtered_frame["__segment_raw"].dropna().tolist() if str(segment).strip()},
        key=lambda segment: _segment_display_label(str(segment)),
    )
    resolved_drilldown_segment = _resolve_segment_value(
        drilldown_segment,
        available_segments,
        DEFAULT_DRILLDOWN_SEGMENT,
    )
    suv_a_segment = _resolve_segment_value("SUV A", available_segments, "SUV A")
    suv_b_segment = _resolve_segment_value("SUV B", available_segments, "SUV B")

    year_text, month_text = resolved_period.split("-", 1)
    previous_year_text = str(int(year_text) - 1)
    month_number = int(month_text)
    metadata = {
        "protocolVersion": "market-scan/v1",
        "requestedPeriod": target_period,
        "resolvedPeriod": resolved_period,
        "selectedTimeRange": {
            "start": custom_periods[0],
            "end": custom_periods[-1],
        } if custom_periods else None,
        "customRangeActive": bool(custom_periods),
        "latestPeriod": available_periods[-1],
        "priorPeriod": prior_period,
        "sameMonthLastYearPeriod": same_month_last_year_period,
        "selectedCountry": selected_country["value"],
        "selectedCountryLabel": selected_country["label"],
        "selectedFuelTypes": selected_fuels,
        "selectedDrilldownSegment": resolved_drilldown_segment,
        "availableCountries": country_options,
        "availablePeriods": [{"value": period, "label": _short_period_label(period)} for period in available_periods],
        "availableFuelTypes": available_fuels,
        "availableSegments": [{"value": segment, "label": _segment_display_label(segment)} for segment in available_segments],
        "labels": {
            "pageTitle": f"{selected_country['label']} {int(year_text)}年{month_number}月市场扫描",
            "currentMonthShort": _short_period_label(resolved_period),
            "previousMonthShort": _short_period_label(prior_period) if prior_period else "-",
            "sameMonthLastYearShort": _short_period_label(same_month_last_year_period) if same_month_last_year_period else "-",
            "currentYtd": f"{year_text[2:4]}YTD",
            "priorYtd": f"{previous_year_text[2:4]}YTD",
            "ytdWindow": f"1-{month_number}月",
        },
    }

    drilldown_segments = list(dict.fromkeys(
        s for s in [resolved_drilldown_segment, suv_a_segment, suv_b_segment]
        if s and s in available_segments
    ))
    drilldown_map = _build_all_drilldowns(
        filtered_frame,
        available_periods=available_periods,
        resolved_period=resolved_period,
        same_month_last_year_period=same_month_last_year_period,
        segment_values=drilldown_segments,
        fuel_panels=DRILLDOWN_PANEL_FUELS,
        ranking_limit=ranking_limit,
        custom_range_periods=custom_periods,
    )

    return {
        "metadata": metadata,
        "results": {
            "overview": _build_overview_payload(
                filtered_frame,
                selected_fuels=selected_fuels,
                available_periods=available_periods,
                resolved_period=resolved_period,
                prior_period=prior_period,
                same_month_last_year_period=same_month_last_year_period,
                ranking_limit=ranking_limit,
                custom_range_periods=custom_periods,
            ),
            "origin": _build_origin_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                prior_period=prior_period,
                same_month_last_year_period=same_month_last_year_period,
                origin_window_months=origin_window_months,
                custom_range_periods=custom_periods,
            ),
            "segment": _build_segment_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                prior_period=prior_period,
                same_month_last_year_period=same_month_last_year_period,
                body_window_months=body_window_months,
                custom_range_periods=custom_periods,
            ),
            "drilldown": drilldown_map.get(resolved_drilldown_segment) or _empty_drilldown_payload(resolved_drilldown_segment),
            "suvA": drilldown_map.get(suv_a_segment) or _empty_drilldown_payload(suv_a_segment),
            "suvB": drilldown_map.get(suv_b_segment) or _empty_drilldown_payload(suv_b_segment),
            "crossTabs": _safe_build_cross_tabs(
                filtered_frame,
                columns=columns,
                selected_fuels=selected_fuels,
                sales_column=_period_to_month_column(resolved_period),
            ),
        },
    }
