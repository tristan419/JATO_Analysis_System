from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import pandas as pd

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
PRIMARY_ORIGINS = ("欧系", "日系", "韩系", "美系", "中系")
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
DEFAULT_DRILLDOWN_SEGMENT = "SUV A0"
FALLBACK_DRILLDOWN_FUELS = ("BEV", "PHEV")
MIN_MARKET_SCAN_RANKING_LIMIT = 10


@dataclass(frozen=True)
class ColumnMap:
    country_value: str
    country_label: str | None
    make: str
    model: str
    origin: str | None
    segment: str
    powertrain: str
    drive_type: str | None
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
        origin=_resolve_existing_column(["车系", "Origin", "Series"], columns),
        segment=segment,
        powertrain=powertrain,
        drive_type=_resolve_existing_column(["Driven wheels", "Drive type", "Drive", "驱动形式"], columns),
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


def _ytd_periods(available_periods: list[str], target_period: str) -> list[str]:
    target = pd.Period(target_period, freq="M")
    return [
        period
        for period in available_periods
        if pd.Period(period, freq="M").year == target.year
        and pd.Period(period, freq="M").month <= target.month
    ]


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
) -> list[dict[str, Any]]:
    current_series = _series_sum(frame, current_columns)
    prior_series = _series_sum(frame, prior_columns)
    prior_month_series = _series_sum(frame, prior_month_columns)
    summary = pd.DataFrame(
        {
            "brand": frame["__brand"],
            "current": current_series,
            "prior": prior_series,
            "prior_month": prior_month_series,
        }
    )
    grouped = summary.groupby("brand", dropna=False)[["current", "prior", "prior_month"]].sum(numeric_only=True)
    grouped = grouped[grouped["current"] > 0].sort_values("current", ascending=False)
    total_current = float(grouped["current"].sum()) or 0.0
    grouped = grouped.head(max(1, int(ranking_limit)))
    items: list[dict[str, Any]] = []
    for rank, (brand, row) in enumerate(grouped.iterrows(), start=1):
        current_value = float(row["current"])
        prior_value = float(row["prior"])
        prior_month_value = float(row["prior_month"])
        items.append(
            {
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


def _build_overview_payload(
    frame: pd.DataFrame,
    selected_fuels: list[str],
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    ranking_limit: int,
) -> dict[str, Any]:
    trend_periods = _window_periods(available_periods, resolved_period, 24)
    trend_columns = [_period_to_month_column(period) for period in trend_periods]
    grouped = _volume_by_group(frame, "__powertrain", trend_columns)
    grouped = grouped.reindex(selected_fuels, fill_value=0.0)

    total_by_column = {column: float(grouped[column].sum()) for column in trend_columns if column in grouped.columns}
    trend_items: list[dict[str, Any]] = []
    for period in trend_periods:
        column = _period_to_month_column(period)
        current_total = total_by_column.get(column, 0.0)
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

    current_month_total = _total_volume(frame, current_period_columns)
    same_month_total = _total_volume(frame, same_month_columns)
    current_ytd_total = _total_volume(frame, current_ytd_columns)
    prior_ytd_total = _total_volume(frame, prior_ytd_columns)

    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)

    return {
        "summary": {
            "headline": f"{_short_period_label(resolved_period)} 总销量 {current_month_total:,.0f} 台，YoY { _delta_payload(current_month_total, same_month_total)['display'] }",
            "subheadline": f"1-{month_number} 月累计 {current_ytd_total:,.0f} 台，累计 YoY { _delta_payload(current_ytd_total, prior_ytd_total)['display'] }",
            "currentMonthVolume": current_month_total,
            "currentMonthYoY": _delta_payload(current_month_total, same_month_total),
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
            ),
        },
    }


def _build_origin_payload(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    origin_window_months: int,
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

    current_total = float(grouped[current_column].sum()) if current_column in grouped.columns else 0.0
    prior_total = float(grouped[prior_column].sum()) if prior_column and prior_column in grouped.columns else 0.0
    same_month_total = float(grouped[same_month_column].sum()) if same_month_column and same_month_column in grouped.columns else 0.0

    summary_frame = pd.DataFrame(index=ordered_origins)
    summary_frame["current"] = grouped[current_column] if current_column in grouped.columns else 0.0
    summary_frame["prior"] = grouped[prior_column] if prior_column and prior_column in grouped.columns else 0.0
    summary_frame["same_month"] = grouped[same_month_column] if same_month_column and same_month_column in grouped.columns else 0.0
    current_ytd_grouped = _volume_by_group(frame, "__origin", current_ytd_columns)
    prior_ytd_grouped = _volume_by_group(frame, "__origin", prior_ytd_columns)
    summary_frame["ytd"] = current_ytd_grouped.sum(axis=1) if not current_ytd_grouped.empty else 0.0
    summary_frame["prior_ytd"] = prior_ytd_grouped.sum(axis=1) if not prior_ytd_grouped.empty else 0.0
    summary_frame = summary_frame.fillna(0.0)

    matrix_rows = []
    for metric_key, label, kind in [
        ("current_volume", "当月销量", "volume"),
        ("mom", "MoM", "delta"),
        ("yoy", "YoY", "delta"),
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
        "matrix": {"columns": ordered_origins, "rows": matrix_rows},
    }


def _build_segment_payload(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    prior_period: str | None,
    same_month_last_year_period: str | None,
    body_window_months: int,
) -> dict[str, Any]:
    working = frame.copy()
    working["__segment_bucket"] = working["__segment_raw"].map(_segment_matrix_bucket)
    working = working[working["__segment_bucket"].notna()].copy()
    if working.empty:
        return {
            "summaryText": "当前筛选下没有可用的细分市场数据。",
            "matrix": {"columns": list(SEGMENT_MATRIX_ORDER), "rows": []},
            "bodyShareTrend": {"items": []},
        }

    current_column = _period_to_month_column(resolved_period)
    prior_column = _period_to_month_column(prior_period) if prior_period else None
    same_month_column = _period_to_month_column(same_month_last_year_period) if same_month_last_year_period else None
    current_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, resolved_period)]
    prior_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, _shift_period(resolved_period, -12))] if same_month_last_year_period else []

    grouped_current = _volume_by_group(working, "__segment_bucket", [current_column])
    grouped_prior = _volume_by_group(working, "__segment_bucket", [prior_column] if prior_column else [])
    grouped_same_month = _volume_by_group(working, "__segment_bucket", [same_month_column] if same_month_column else [])
    grouped_ytd = _volume_by_group(working, "__segment_bucket", current_ytd_columns)
    grouped_prior_ytd = _volume_by_group(working, "__segment_bucket", prior_ytd_columns)

    matrix_rows = []
    for metric_key, label in [
        ("current_volume", "当月销量"),
        ("mom", "MoM"),
        ("yoy", "YoY"),
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
            if metric_key == "current_volume":
                payload = _metric_cell(current_value, "volume")
            elif metric_key == "mom":
                payload = _delta_payload(current_value, prior_value)
            elif metric_key == "yoy":
                payload = _delta_payload(current_value, same_month_value)
            elif metric_key == "ytd":
                payload = _metric_cell(ytd_value, "volume")
            else:
                payload = _delta_payload(ytd_value, prior_ytd_value)
            cells.append({"key": bucket, **payload})
        matrix_rows.append({"metricKey": metric_key, "label": label, "cells": cells})

    trend_periods = _window_periods(available_periods, resolved_period, body_window_months)
    trend_items = []
    for period in trend_periods:
        column = _period_to_month_column(period)
        suv_volume = float(working.loc[working["__segment_bucket"].str.startswith("SUV"), column].sum()) if column in working.columns else 0.0
        sedan_volume = float(working.loc[working["__segment_bucket"].str.startswith("SD"), column].sum()) if column in working.columns else 0.0
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
                }
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


def _build_drilldown_payload(
    frame: pd.DataFrame,
    available_periods: list[str],
    resolved_period: str,
    same_month_last_year_period: str | None,
    segment_value: str,
    fuel_panels: tuple[str, ...],
    ranking_limit: int,
) -> dict[str, Any]:
    segment_frame = frame[frame["__segment_raw"] == segment_value].copy()
    if segment_frame.empty:
        return {
            "segment": segment_value,
            "segmentLabel": _segment_display_label(segment_value),
            "summaryText": "当前筛选下没有该细分市场的可用数据。",
            "totalRanking": {"items": []},
            "ytdFuelTrend": {"items": []},
            "fuelPanels": [],
        }

    year_text, month_text = resolved_period.split("-", 1)
    month_number = int(month_text)
    current_month_columns = [_period_to_month_column(resolved_period)]
    same_month_columns = [_period_to_month_column(same_month_last_year_period)] if same_month_last_year_period else []
    current_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, resolved_period)]
    prior_ytd_columns = [_period_to_month_column(period) for period in _ytd_periods(available_periods, _shift_period(resolved_period, -12))] if same_month_last_year_period else []
    available_fuels = _available_fuel_types(segment_frame)

    total_ranking = _build_total_ranking_items(
        segment_frame,
        current_columns=current_ytd_columns,
        prior_columns=prior_ytd_columns,
        fuel_order=available_fuels,
        ranking_limit=ranking_limit,
    )
    segment_total_ytd = _total_volume(segment_frame, current_ytd_columns)
    segment_total_month = _total_volume(segment_frame, current_month_columns)
    fuel_panel_items = []
    for fuel_type in fuel_panels:
        fuel_panel_items.append(
            {
                "fuelType": fuel_type,
                "ytdTitle": f"{fuel_type} 1-{month_number}月累计",
                "monthTitle": f"{fuel_type} { _short_period_label(resolved_period) }",
                "ytdRanking": _build_single_fuel_ranking_items(
                    segment_frame,
                    fuel_type=fuel_type,
                    current_columns=current_ytd_columns,
                    prior_columns=prior_ytd_columns,
                    segment_total=segment_total_ytd,
                    ranking_limit=ranking_limit,
                ),
                "monthRanking": _build_single_fuel_ranking_items(
                    segment_frame,
                    fuel_type=fuel_type,
                    current_columns=current_month_columns,
                    prior_columns=same_month_columns,
                    segment_total=segment_total_month,
                    ranking_limit=ranking_limit,
                ),
            }
        )

    headline_model = total_ranking[0]["model"] if total_ranking else "市场"
    headline_yoy = total_ranking[0]["yoy"]["display"] if total_ranking else "-"
    return {
        "segment": segment_value,
        "segmentLabel": _segment_display_label(segment_value),
        "title": f"{_segment_display_label(segment_value)} 车型 {year_text}年1-{month_number}月",
        "summaryText": f"{headline_model} 目前领跑 {_segment_display_label(segment_value)}，累计同比 {headline_yoy}。",
        "totalRanking": {
            "title": "YTD Total Model Ranking",
            "items": total_ranking,
        },
        "ytdFuelTrend": _build_ytd_fuel_trend(segment_frame, available_fuels, resolved_period, available_periods),
        "fuelPanels": fuel_panel_items,
    }


def query_market_scan_deck(
    country: str | None,
    target_period: str | None,
    fuel_types: list[str],
    trend_window_months: int,
    origin_window_months: int,
    body_window_months: int,
    ranking_limit: int,
    drilldown_segment: str | None,
) -> dict[str, Any]:
    ranking_limit = max(MIN_MARKET_SCAN_RANKING_LIMIT, int(ranking_limit))
    cache_key = f"{country}|{target_period}|{','.join(sorted(fuel_types))}|{trend_window_months}|{origin_window_months}|{body_window_months}|{ranking_limit}|{drilldown_segment}"
    now = time.monotonic()
    dataset_token = repo.current_dataset_token()
    cached = _deck_cache.get(cache_key)
    if cached is not None:
        cached_at, cached_token, cached_result = cached
        if cached_token == dataset_token and (now - cached_at) < _DECK_CACHE_TTL_SECONDS:
            return cached_result

    result = _query_market_scan_deck_impl(
        country, target_period, fuel_types,
        trend_window_months, origin_window_months, body_window_months,
        ranking_limit, drilldown_segment,
    )

    with _deck_cache_lock:
        _deck_cache[cache_key] = (now, dataset_token, result)
        if len(_deck_cache) > 32:
            oldest_key = min(_deck_cache, key=lambda k: _deck_cache[k][0])
            _deck_cache.pop(oldest_key, None)

    return result


def _query_market_scan_deck_impl(
    country: str | None,
    target_period: str | None,
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
            ),
            "origin": _build_origin_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                prior_period=prior_period,
                same_month_last_year_period=same_month_last_year_period,
                origin_window_months=origin_window_months,
            ),
            "segment": _build_segment_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                prior_period=prior_period,
                same_month_last_year_period=same_month_last_year_period,
                body_window_months=body_window_months,
            ),
            "drilldown": _build_drilldown_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                same_month_last_year_period=same_month_last_year_period,
                segment_value=resolved_drilldown_segment,
                fuel_panels=DRILLDOWN_PANEL_FUELS,
                ranking_limit=ranking_limit,
            ),
            "suvA": _build_drilldown_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                same_month_last_year_period=same_month_last_year_period,
                segment_value=suv_a_segment,
                fuel_panels=DRILLDOWN_PANEL_FUELS,
                ranking_limit=ranking_limit,
            ),
            "suvB": _build_drilldown_payload(
                filtered_frame,
                available_periods=available_periods,
                resolved_period=resolved_period,
                same_month_last_year_period=same_month_last_year_period,
                segment_value=suv_b_segment,
                fuel_panels=DRILLDOWN_PANEL_FUELS,
                ranking_limit=ranking_limit,
            ),
        },
    }
