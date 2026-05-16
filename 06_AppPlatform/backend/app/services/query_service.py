from uuid import uuid4
import io
import re
import threading
import time
from typing import Literal

import numpy as np
import pandas as pd

from app.core.config import (
    DEFAULT_GROUP_BY,
    FILTER_OPTIONS_SNAPSHOT_TTL_SECONDS,
)
from app.infra import parquet_repository as repo


# ── Column name candidates ──────────────────────────────────────
COUNTRY_CANDIDATES = ["国家", "Country", "country"]
SEGMENT_CANDIDATES = ["细分市场（按车长）", "细分市场", "segment"]
POWERTRAIN_CANDIDATES = ["动总规整", "powertrain", "Powertrain"]
MAKE_CANDIDATES = ["Make", "品牌", "make"]
MODEL_CANDIDATES = ["Model", "model"]
VERSION_CANDIDATES = ["Version name", "version name", "Version Name", "Version"]
MSRP_CANDIDATES = [
    "MSRP规整",
    "MSRP including delivery charge",
    "MSRP",
    "MSRP区间",
]
LENGTH_CANDIDATES = [
    "length (mm)",
    "车长(mm)",
    "车长",
    "length",
]
BATTERY_RANGE_CANDIDATES = [
    "Battery range",
    "Battery Range",
    "battery range",
    "WLTP range",
    "EV range",
    "续航里程",
    "电池续航",
]
BATTERY_CAPACITY_CANDIDATES = [
    "Battery kwh",
    "Battery kWh",
    "Useable battery kilowatt hour (kWh)",
    "Battery capacity",
    "Battery Capacity",
    "电池容量",
]
POWERTRAIN_DISPLAY_ORDER = ["BEV", "PHEV", "MHEV", "HEV", "ICE"]
POWERTRAIN_ENERGY_FACTOR = {
    "BEV": 0.55, "PHEV": 0.85, "HEV": 0.90, "MHEV": 1.00, "ICE": 1.10,
}
TOP_LEVEL_FILTER_CANDIDATE_SETS = [
    COUNTRY_CANDIDATES,
    SEGMENT_CANDIDATES,
    POWERTRAIN_CANDIDATES,
]

_top_level_filter_options_cache: (
    tuple[float, str, dict[str, list[str]]] | None
) = None
_top_level_filter_options_lock = threading.Lock()
_OVERVIEW_CACHE_TTL_SECONDS = 300
_overview_cache: dict[
    tuple[
        tuple[tuple[str, tuple[str, ...]], ...],
        bool,
        int,
    ],
    tuple[float, str, dict],
] = {}
_overview_cache_lock = threading.Lock()


# ── Helper: resolve column name ────────────────────────────────
def _resolve_existing_column(
    candidates: list[str],
    columns: list[str],
) -> str | None:
    normalized = {
        str(col).strip().lower(): str(col).strip()
        for col in columns
    }
    for candidate in candidates:
        hit = normalized.get(str(candidate).strip().lower())
        if hit:
            return hit
    return None


def _has_active_filters(filters: dict[str, list[str]]) -> bool:
    for values in filters.values():
        for value in values or []:
            if str(value).strip():
                return True
    return False


def _normalize_query_cache_filters(
    filters: dict[str, list[str]],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    normalized_filters: list[tuple[str, tuple[str, ...]]] = []
    for column, values in filters.items():
        normalized_column = str(column).strip()
        normalized_values = tuple(
            sorted(
                {
                    str(value).strip()
                    for value in (values or [])
                    if value is not None and str(value).strip()
                }
            )
        )
        if not normalized_column or not normalized_values:
            continue
        normalized_filters.append((normalized_column, normalized_values))
    return tuple(sorted(normalized_filters))


def _resolve_option_columns(
    candidate_sets: list[list[str]],
    columns: list[str],
) -> list[str]:
    resolved_columns: list[str] = []
    for candidates in candidate_sets:
        hit = _resolve_existing_column(candidates, columns)
        if hit and hit not in resolved_columns:
            resolved_columns.append(hit)
    return resolved_columns


def _load_top_level_filter_options_snapshot() -> dict[str, list[str]]:
    global _top_level_filter_options_cache

    now = time.monotonic()
    dataset_token = repo.current_dataset_token()
    if _top_level_filter_options_cache is not None:
        (
            cached_at,
            cached_token,
            cached_snapshot,
        ) = _top_level_filter_options_cache
        if (
            cached_token == dataset_token
            and (now - cached_at) < FILTER_OPTIONS_SNAPSHOT_TTL_SECONDS
        ):
            return cached_snapshot

    with _top_level_filter_options_lock:
        now = time.monotonic()
        dataset_token = repo.current_dataset_token()
        if _top_level_filter_options_cache is not None:
            (
                cached_at,
                cached_token,
                cached_snapshot,
            ) = _top_level_filter_options_cache
            if (
                cached_token == dataset_token
                and (now - cached_at) < FILTER_OPTIONS_SNAPSHOT_TTL_SECONDS
            ):
                return cached_snapshot

        columns = repo.list_columns()
        top_level_columns = _resolve_option_columns(
            TOP_LEVEL_FILTER_CANDIDATE_SETS,
            columns,
        )
        snapshot = repo.load_distinct_options_batch(top_level_columns, {})
        _top_level_filter_options_cache = (now, dataset_token, snapshot)
        return snapshot


def _year_columns(columns: list[str]) -> list[str]:
    return sorted(c for c in columns if len(c) == 4 and c.isdigit())


_MONTH_ORDER = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_MONTH_LABEL_BY_NUMBER = {
    value: key for key, value in _MONTH_ORDER.items()
}

_GROUPED_SHARE_MODE_SPECS = {
    "四驱占比": {
        "column_candidates": ["Driven wheels"],
        "series": [
            (
                "4x4",
                lambda values: values.astype(str).str.strip().str.lower().eq("4x4"),
            ),
        ],
    },
    "Business/Private 占比": {
        "column_candidates": ["Registration type"],
        "series": [
            (
                "Business",
                lambda values: values.astype(str).str.strip().str.lower().eq("business"),
            ),
            (
                "Private",
                lambda values: values.astype(str).str.strip().str.lower().eq("private"),
            ),
        ],
    },
}

_GROUPED_SHARE_SPLIT_SPECS = {
    "segment": {
        "column_candidates": ["细分市场（按车长）"],
    },
    "powertrain": {
        "column_candidates": ["动总规整"],
    },
}


def _sort_month_cols_chrono(cols: list[str]) -> list[str]:
    def _key(col: str) -> tuple[int, int]:
        parts = col.split(" ", 1)
        return (int(parts[0]), _MONTH_ORDER.get(parts[1], 0)) if len(parts) == 2 else (0, 0)
    return sorted(cols, key=_key)


def _month_columns(columns: list[str]) -> list[str]:
    return _sort_month_cols_chrono(
        [c for c in columns
         if len(c) >= 8 and c[:4].isdigit() and " " in c]
    )


def _supplemental_month_year_groups(
    columns: list[str],
) -> dict[str, list[str]]:
    explicit_years = set(_year_columns(columns))
    grouped: dict[str, list[str]] = {}
    for column in _month_columns(columns):
        year_label = column.split(" ", 1)[0].strip()
        if not year_label.isdigit() or year_label in explicit_years:
            continue
        grouped.setdefault(year_label, []).append(column)
    return {
        year_label: grouped[year_label]
        for year_label in sorted(grouped.keys(), key=int)
    }


def _prepare_effective_year_value_columns(
    df: pd.DataFrame,
    supplemental_year_groups: dict[str, list[str]],
) -> tuple[pd.DataFrame, dict[str, str]]:
    effective = {
        year_label: year_label
        for year_label in _year_columns(df.columns.astype(str).tolist())
        if year_label in df.columns
    }
    for year_label, month_columns in supplemental_year_groups.items():
        present_columns = [col for col in month_columns if col in df.columns]
        if not present_columns:
            continue
        derived_column = f"__derived_year_{year_label}"
        df[derived_column] = _sum_sales_columns(df, present_columns)
        effective[year_label] = derived_column
    ordered = {
        year_label: effective[year_label]
        for year_label in sorted(effective.keys(), key=int)
    }
    return df, ordered


def _sum_sales_columns(df: pd.DataFrame, year_cols: list[str]) -> pd.Series:
    """Sum across year columns to get total sales per row."""
    total = pd.Series(0.0, index=df.index)
    for yc in year_cols:
        if yc in df.columns:
            total += pd.to_numeric(df[yc], errors="coerce").fillna(0)
    return total


def _sales_columns_for_scope(
    columns: list[str],
    *,
    year: int | None = None,
    month: int | None = None,
    default_latest_year: bool = False,
) -> tuple[list[str], list[str], str | None]:
    explicit_years = _year_columns(columns)
    supplemental_year_groups = _supplemental_month_year_groups(columns)
    month_columns = _month_columns(columns)
    available_years = sorted(
        {str(item) for item in explicit_years}
        | set(supplemental_year_groups.keys()),
        key=int,
    )

    resolved_year: str | None = None
    if year is not None:
        resolved_year = str(int(year))
    elif default_latest_year and available_years:
        resolved_year = available_years[-1]

    if resolved_year:
        if month is not None:
            month_label = _MONTH_LABEL_BY_NUMBER.get(int(month))
            target_column = (
                f"{resolved_year} {month_label}" if month_label else ""
            )
            if target_column in month_columns:
                return [target_column], available_years, resolved_year
            return [], available_years, resolved_year

        if resolved_year in explicit_years:
            return [resolved_year], available_years, resolved_year
        if resolved_year in supplemental_year_groups:
            return (
                supplemental_year_groups[resolved_year],
                available_years,
                resolved_year,
            )
        return [], available_years, resolved_year

    return explicit_years, available_years, None


def _parse_time_label(
    label: object,
) -> tuple[Literal["year", "month", "quarter"], int, int | None] | None:
    text = str(label).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}", text):
        return ("year", int(text), None)

    month_name_match = re.fullmatch(r"(\d{4})\s+([A-Za-z]{3})", text)
    if month_name_match:
        month = _MONTH_ORDER.get(month_name_match.group(2).title())
        if month is None:
            return None
        return ("month", int(month_name_match.group(1)), month)

    short_year_match = re.fullmatch(r"(\d{2})[./-](\d{1,2})", text)
    if short_year_match:
        return (
            "month",
            2000 + int(short_year_match.group(1)),
            int(short_year_match.group(2)),
        )

    numeric_match = re.fullmatch(r"(\d{4})[./-](\d{1,2})", text)
    if numeric_match:
        return (
            "month",
            int(numeric_match.group(1)),
            int(numeric_match.group(2)),
        )

    quarter_match = re.fullmatch(r"(\d{4})-Q([1-4])", text)
    if quarter_match:
        return (
            "quarter",
            int(quarter_match.group(1)),
            int(quarter_match.group(2)),
        )
    return None


def _time_label_ordinal(label: object) -> int | None:
    parsed = _parse_time_label(label)
    if parsed is None:
        return None
    kind, year, unit = parsed
    if kind == "year":
        return year * 100 + 12
    if kind == "quarter":
        return year * 100 + int(unit or 0) * 3
    return year * 100 + int(unit or 0)


def _extract_time_range(
    time_range: object,
) -> tuple[str, str] | None:
    if not isinstance(time_range, dict):
        return None
    start_text = str(time_range.get("start", "")).strip()
    end_text = str(time_range.get("end", "")).strip()
    if not start_text or not end_text:
        return None
    start_ord = _time_label_ordinal(start_text)
    end_ord = _time_label_ordinal(end_text)
    if start_ord is None or end_ord is None:
        return None
    if start_ord <= end_ord:
        return start_text, end_text
    return end_text, start_text


def _sales_columns_for_time_label(columns: list[str], label: str) -> list[str]:
    parsed = _parse_time_label(label)
    if parsed is None:
        return []
    kind, year, unit = parsed
    if kind == "year":
        cols, _, _ = _sales_columns_for_scope(columns, year=year)
        return cols
    if kind == "quarter":
        start_month = (int(unit or 1) - 1) * 3 + 1
        end_month = start_month + 2
        selected: list[str] = []
        for month in range(start_month, end_month + 1):
            cols, _, _ = _sales_columns_for_scope(columns, year=year, month=month)
            selected.extend(cols)
        return list(dict.fromkeys(selected))
    cols, _, _ = _sales_columns_for_scope(columns, year=year, month=int(unit or 1))
    return cols


def _sales_columns_for_time_range(
    columns: list[str],
    start_label: str,
    end_label: str,
) -> list[str]:
    normalized_range = _extract_time_range({"start": start_label, "end": end_label})
    if normalized_range is None:
        return []
    normalized_start, normalized_end = normalized_range
    start_parsed = _parse_time_label(normalized_start)
    end_parsed = _parse_time_label(normalized_end)
    if start_parsed is None or end_parsed is None:
        return []

    if start_parsed[0] == "year" and end_parsed[0] == "year":
        selected: list[str] = []
        explicit_years = set(_year_columns(columns))
        supplemental_year_groups = _supplemental_month_year_groups(columns)
        for year in range(start_parsed[1], end_parsed[1] + 1):
            year_label = str(year)
            if year_label in explicit_years:
                selected.append(year_label)
            elif year_label in supplemental_year_groups:
                selected.extend(supplemental_year_groups[year_label])
        return list(dict.fromkeys(selected))

    start_ord = _time_label_ordinal(normalized_start)
    end_ord = _time_label_ordinal(normalized_end)
    if start_ord is None or end_ord is None:
        return []
    return [
        column
        for column in _month_columns(columns)
        if (column_ord := _time_label_ordinal(column)) is not None
        and start_ord <= column_ord <= end_ord
    ]


def _filter_time_labels(
    labels: list[str],
    time_range: dict[str, str] | None,
) -> list[str]:
    normalized_range = _extract_time_range(time_range)
    if normalized_range is None:
        return labels
    start_ord = _time_label_ordinal(normalized_range[0])
    end_ord = _time_label_ordinal(normalized_range[1])
    if start_ord is None or end_ord is None:
        return labels
    return [
        label
        for label in labels
        if (label_ord := _time_label_ordinal(label)) is not None
        and start_ord <= label_ord <= end_ord
    ]


def _query_grouped_share_time_series(
    filters: dict[str, list[str]],
    grain: str,
    group_by: str,
    share_split_by: str | None = None,
    time_range: dict[str, str] | None = None,
) -> dict:
    spec = _GROUPED_SHARE_MODE_SPECS.get(group_by)
    if spec is None:
        return {"grain": grain, "rows": 0, "items": []}

    columns_all = repo.list_columns()
    group_column = _resolve_existing_column(spec["column_candidates"], columns_all)
    split_spec = _GROUPED_SHARE_SPLIT_SPECS.get(str(share_split_by or "").strip().lower())
    split_column = (
        _resolve_existing_column(split_spec["column_candidates"], columns_all)
        if split_spec is not None
        else None
    )
    normalized_grain = "year" if str(grain).lower() == "year" else "month"
    if not group_column:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    supplemental_year_groups: dict[str, list[str]] = {}
    if normalized_grain == "year":
        time_cols = _year_columns(columns_all)
        supplemental_year_groups = _supplemental_month_year_groups(columns_all)
        month_supplement_columns = [
            column
            for month_columns in supplemental_year_groups.values()
            for column in month_columns
        ]
        load_cols = [group_column, *time_cols, *month_supplement_columns]
    else:
        time_cols = _month_columns(columns_all)
        load_cols = [group_column, *time_cols]
    if split_column and split_column not in load_cols:
        load_cols = [split_column, *load_cols]

    if len(load_cols) <= 1:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    df = repo.load_slice(columns=load_cols, filters=filters, limit=None, offset=0)
    if df.empty or group_column not in df.columns:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    df[group_column] = df[group_column].astype(str).fillna("?").str.strip()
    if split_column and split_column in df.columns:
        df[split_column] = df[split_column].astype(str).fillna("未标注").str.strip()
    numeric_columns = [
        column
        for column in load_cols
        if column not in {group_column, split_column}
    ]
    for tc in numeric_columns:
        if tc in df.columns:
            df[tc] = pd.to_numeric(df[tc], errors="coerce").fillna(0)

    if normalized_grain == "year":
        df, effective_year_columns = _prepare_effective_year_value_columns(
            df,
            supplemental_year_groups,
        )
        time_labels = _filter_time_labels(list(effective_year_columns.keys()), time_range)
        value_columns = [
            effective_year_columns[label]
            for label in time_labels
            if label in effective_year_columns
        ]
    else:
        time_labels = _filter_time_labels(time_cols, time_range)
        value_columns = [label for label in time_labels if label in df.columns]

    if not value_columns:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    items: list[dict[str, object]] = []
    series_count = len(spec["series"])
    split_frames: list[tuple[str | None, pd.DataFrame]]
    if split_column and split_column in df.columns:
        split_totals = (
            df.groupby(split_column)[value_columns]
            .sum()
            .sum(axis=1)
            .sort_values(ascending=False)
        )
        split_frames = [
            (str(split_value), df[df[split_column] == split_value].copy())
            for split_value in split_totals.index
        ]
    else:
        split_frames = [(None, df)]
    for split_value, frame in split_frames:
        totals = frame.loc[:, value_columns].sum()
        for series_name, matcher in spec["series"]:
            mask = matcher(frame[group_column])
            numerator = (
                frame.loc[mask, value_columns].sum()
                if mask.any()
                else pd.Series(0.0, index=value_columns)
            )
            resolved_series_name = (
                series_name
                if split_value is None
                else split_value
                if series_count == 1
                else f"{split_value} · {series_name}"
            )
            for time_label, value_column in zip(time_labels, value_columns):
                denominator = float(totals.get(value_column, 0.0))
                share_value = float(numerator.get(value_column, 0.0))
                items.append(
                    {
                        "time": time_label,
                        "value": round((share_value / denominator * 100.0), 4) if denominator > 0 else 0.0,
                        "series": resolved_series_name,
                    }
                )
    return {"grain": normalized_grain, "rows": len(items), "items": items}


def _resolve_sales_columns_from_options(
    columns: list[str],
    opts: dict | None,
) -> list[str]:
    scope_opts = opts or {}
    normalized_range = _extract_time_range(scope_opts.get("time_range"))
    if normalized_range is not None:
        return _sales_columns_for_time_range(
            columns,
            normalized_range[0],
            normalized_range[1],
        )
    sales_columns, _, _ = _sales_columns_for_scope(
        columns,
        year=scope_opts.get("sales_year"),
        month=scope_opts.get("sales_month"),
        default_latest_year=bool(scope_opts.get("default_latest_year")),
    )
    return sales_columns


def _has_explicit_time_range(opts: dict | None) -> bool:
    scope_opts = opts or {}
    return _extract_time_range(scope_opts.get("time_range")) is not None


def _aggregate_sales(
    filters: dict[str, list[str]],
    target_column: str,
    top_n: int,
    sales_columns: list[str],
) -> list[dict]:
    if not sales_columns:
        return []
    frame = repo.load_slice(
        columns=[target_column, *sales_columns],
        filters=filters,
        limit=200_000,
        offset=0,
    )
    if frame.empty or target_column not in frame.columns:
        return []

    ranking = pd.DataFrame()
    ranking["label"] = frame[target_column].astype(str).str.strip()
    ranking.loc[ranking["label"] == "", "label"] = pd.NA
    ranking["value"] = _sum_sales_columns(frame, sales_columns)
    ranking = ranking.dropna(subset=["label"])
    if ranking.empty:
        return []

    grouped = (
        ranking.groupby("label", as_index=False)["value"]
        .sum()
        .sort_values(["value", "label"], ascending=[False, True])
        .head(max(1, int(top_n)))
    )
    return [
        {"label": str(row["label"]), "value": float(row["value"])}
        for _, row in grouped.iterrows()
    ]


def _make_price_bands(series: pd.Series, band_size: int) -> pd.Series:
    band_size = max(100, int(band_size))
    return (series // band_size * band_size).astype(int)


def _make_length_bands(series: pd.Series, band_size: int) -> pd.Series:
    band_size = max(50, int(band_size))
    return (series // band_size * band_size).astype(int)


def _weighted_median(values: pd.Series, weights: pd.Series) -> float:
    """Value at cumulative-weight = 50 %, weighted by *weights*."""
    tmp = pd.DataFrame({"v": values, "w": weights}).dropna()
    tmp = tmp[tmp["w"] > 0]
    if tmp.empty:
        return float(values.median()) if len(values) else 0.0
    tmp = tmp.sort_values("v")
    cutoff = tmp["w"].sum() / 2.0
    return float(tmp.loc[(tmp["w"].cumsum() >= cutoff).idxmax(), "v"])


def _weighted_quantile(values: pd.Series, weights: pd.Series, quantile: float) -> float:
    """Value at cumulative-weight = *quantile*, weighted by *weights*."""
    quantile = min(max(float(quantile), 0.0), 1.0)
    tmp = pd.DataFrame({"v": values, "w": weights}).dropna()
    tmp = tmp[tmp["w"] > 0]
    if tmp.empty:
        if len(values) == 0:
            return 0.0
        return float(values.quantile(quantile))
    tmp = tmp.sort_values("v")
    cutoff = tmp["w"].sum() * quantile
    if cutoff <= 0:
        return float(tmp.iloc[0]["v"])
    return float(tmp.loc[(tmp["w"].cumsum() >= cutoff).idxmax(), "v"])


def _build_peer_corridor(
    agg: pd.DataFrame,
    *,
    target_length: float | None,
    target_msrp: float | None,
) -> dict | None:
    if agg.empty or "Length" not in agg.columns or "MSRP" not in agg.columns:
        return None

    peer = agg.dropna(subset=["Length", "MSRP"]).copy()
    peer = peer[(peer["Length"] > 0) & (peer["MSRP"] > 0)]
    if peer.empty:
        return None

    weights = pd.to_numeric(peer.get("Sales"), errors="coerce").fillna(0).clip(lower=0)
    weights = weights.where(weights > 0, 1.0)
    peer["PricePerMeter"] = peer["MSRP"] / (peer["Length"] / 1000.0)

    msrp_p25 = _weighted_quantile(peer["MSRP"], weights, 0.25)
    msrp_median = _weighted_quantile(peer["MSRP"], weights, 0.50)
    msrp_p75 = _weighted_quantile(peer["MSRP"], weights, 0.75)
    ppm_median = _weighted_quantile(peer["PricePerMeter"], weights, 0.50)

    target_price_per_meter = None
    target_residual = None
    target_residual_pct = None
    target_ppm_residual_pct = None
    position_label = "unknown"
    stance_code = "unscored"
    stance_label = "待判断"
    stance_detail = "当前没有足够目标定价来判断 price stance。"
    if target_msrp is not None and target_msrp > 0:
        target_residual = float(target_msrp) - msrp_median
        if msrp_median > 0:
            target_residual_pct = target_residual / msrp_median * 100.0
        if target_length is not None and target_length > 0:
            target_price_per_meter = float(target_msrp) / (float(target_length) / 1000.0)
            if ppm_median > 0:
                target_ppm_residual_pct = (
                    (target_price_per_meter - ppm_median) / ppm_median * 100.0
                )
        if target_msrp < msrp_p25:
            position_label = "below-peer-range"
        elif target_msrp > msrp_p75:
            position_label = "above-peer-range"
        else:
            position_label = "within-peer-range"

        gap_pct = float(target_residual_pct or 0.0)
        if gap_pct <= -12 or (position_label == "below-peer-range" and gap_pct <= -6):
            stance_code = "aggressive-share-take"
            stance_label = "进攻切入价"
            stance_detail = "明显低于 peer 中位数，更偏 volume / share take。"
        elif gap_pct <= -4:
            stance_code = "competitive-entry"
            stance_label = "偏进攻主流价"
            stance_detail = "略低于 peer 中位数，属于更积极的切入位。"
        elif gap_pct < 8 and position_label == "within-peer-range":
            stance_code = "market-aligned"
            stance_label = "主流防守价"
            stance_detail = "落在 peer corridor 中段，更像市场主流防守位。"
        elif gap_pct < 15:
            stance_code = "upper-band-stretch"
            stance_label = "偏高试探价"
            stance_detail = "已靠近或略高于 corridor 上沿，需要更强产品力支撑。"
        else:
            stance_code = "premium-stretch"
            stance_label = "高溢价试探价"
            stance_detail = "明显高于 peer corridor，属于 premium stretch。"

    return {
        "peerCount": int(len(peer)),
        "salesTotal": float(weights.sum()),
        "lengthMin": int(round(float(peer["Length"].min()))),
        "lengthMax": int(round(float(peer["Length"].max()))),
        "msrpP25": float(msrp_p25),
        "msrpMedian": float(msrp_median),
        "msrpP75": float(msrp_p75),
        "pricePerMeterMedian": float(ppm_median),
        "targetLength": float(target_length) if target_length is not None else None,
        "targetMsrp": float(target_msrp) if target_msrp is not None else None,
        "targetPricePerMeter": float(target_price_per_meter) if target_price_per_meter is not None else None,
        "targetResidual": float(target_residual) if target_residual is not None else None,
        "targetResidualPct": float(target_residual_pct) if target_residual_pct is not None else None,
        "targetPricePerMeterResidualPct": (
            float(target_ppm_residual_pct) if target_ppm_residual_pct is not None else None
        ),
        "positionLabel": position_label,
        "stanceCode": stance_code,
        "stanceLabel": stance_label,
        "stanceDetail": stance_detail,
        "salesWeighted": True,
    }


def _load_excluding_zero_sales(
    filters: dict[str, list[str]],
    selected_columns: list[str] | None,
) -> pd.DataFrame:
    """Load rows that have at least some sales across all time columns."""
    all_cols = repo.list_columns()
    time_cols = _year_columns(all_cols) + _month_columns(all_cols)
    load_cols = list(dict.fromkeys((selected_columns or all_cols) + time_cols))
    rows = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if rows.empty or not time_cols:
        return rows
    tc_present = [tc for tc in time_cols if tc in rows.columns]
    if tc_present:
        for tc in tc_present:
            rows[tc] = pd.to_numeric(rows[tc], errors="coerce").fillna(0)
        rows = rows[rows[tc_present].sum(axis=1) > 0]
    if selected_columns:
        rows = rows[[c for c in selected_columns if c in rows.columns]]
    return rows


# ── Metadata / Filters ─────────────────────────────────────────
def metadata_columns() -> list[str]:
    return repo.list_columns()


def filters_options(column: str, filters: dict[str, list[str]]) -> dict:
    if not _has_active_filters(filters):
        snapshot = _load_top_level_filter_options_snapshot()
        if column in snapshot:
            return {
                "column": column,
                "options": snapshot[column],
            }
    return {
        "column": column,
        "options": repo.load_distinct_options(column, filters),
    }


# ── Analysis query ──────────────────────────────────────────────
def query_analysis(
    filters: dict[str, list[str]],
    group_by: str | None,
    metric_candidates: list[str],
    top_n: int,
    prefer_precomputed: bool,
) -> dict:
    filter_count = sum(1 for _, vals in filters.items() if vals)

    if prefer_precomputed and filter_count <= 1:
        pre = repo.load_precomputed("country")
        if not pre.empty:
            return {
                "route": "precomputed",
                "rows": int(len(pre)),
                "items": pre.to_dict(orient="records"),
            }

    group_key = group_by or DEFAULT_GROUP_BY
    grouped = repo.aggregate(
        group_by=group_key,
        metric_candidates=metric_candidates,
        filters=filters,
        top_n=top_n,
    )
    if not grouped.empty and filter_count >= 1:
        return {
            "route": "dynamic-aggregate",
            "groupBy": group_key,
            "rows": int(len(grouped)),
            "items": grouped.to_dict(orient="records"),
        }

    columns = [group_key, *metric_candidates] if metric_candidates else None
    raw = repo.load_slice(columns=columns, filters=filters)
    return {
        "route": "raw",
        "rows": int(len(raw)),
        "items": raw.to_dict(orient="records"),
    }


# ── Time series (single series) ────────────────────────────────
def query_time_series(
    filters: dict[str, list[str]],
    grain: str,
    top_n: int,
) -> dict:
    normalized_grain = "year" if str(grain).lower() == "year" else "month"
    if normalized_grain == "month":
        series_df = repo.time_series(
            filters=filters,
            grain=normalized_grain,
            top_n=top_n,
        )
    else:
        columns_all = repo.list_columns()
        year_columns = _year_columns(columns_all)
        supplemental_year_groups = _supplemental_month_year_groups(
            columns_all,
        )
        load_columns = [*year_columns]
        for month_columns in supplemental_year_groups.values():
            load_columns.extend(month_columns)

        if not load_columns:
            series_df = pd.DataFrame(columns=["time", "value"])
        else:
            frame = repo.load_slice(
                columns=load_columns,
                filters=filters,
                limit=200_000,
                offset=0,
            )
            if frame.empty:
                series_df = pd.DataFrame(columns=["time", "value"])
            else:
                frame, effective_year_columns = (
                    _prepare_effective_year_value_columns(
                        frame,
                        supplemental_year_groups,
                    )
                )
                payload = []
                for year_label, source_column in effective_year_columns.items():
                    payload.append(
                        {
                            "time": year_label,
                            "value": float(
                                pd.to_numeric(
                                    frame[source_column],
                                    errors="coerce",
                                ).sum(skipna=True)
                            ),
                        }
                    )
                series_df = pd.DataFrame(payload)

    return {
        "grain": normalized_grain,
        "rows": int(len(series_df)),
        "items": series_df.head(max(1, int(top_n))).to_dict(
            orient="records"
        ),
    }


# ── Grouped time series (multi-series with group-by, top-n, "其他") ──
def query_grouped_time_series(
    filters: dict[str, list[str]],
    grain: str,
    group_by: str | None,
    top_n: int,
    include_others: bool,
    share_split_by: str | None = None,
    time_range: dict[str, str] | None = None,
) -> dict:
    if not group_by:
        ts = query_time_series(filters, grain, 120)
        items = [
            {"time": r["time"], "value": r["value"], "series": "总和"}
            for r in ts["items"]
        ]
        allowed_labels = set(
            _filter_time_labels(
                [str(item["time"]) for item in items],
                time_range,
            )
        )
        if time_range is not None:
            items = [item for item in items if str(item["time"]) in allowed_labels]
        return {"grain": grain, "rows": len(items), "items": items}
    if group_by in _GROUPED_SHARE_MODE_SPECS:
        return _query_grouped_share_time_series(
            filters=filters,
            grain=grain,
            group_by=group_by,
            share_split_by=share_split_by,
            time_range=time_range,
        )

    columns_all = repo.list_columns()
    normalized_grain = "year" if str(grain).lower() == "year" else "month"
    supplemental_year_groups: dict[str, list[str]] = {}
    if normalized_grain == "year":
        time_cols = _year_columns(columns_all)
        supplemental_year_groups = _supplemental_month_year_groups(
            columns_all,
        )
        month_supplement_columns = [
            column
            for month_columns in supplemental_year_groups.values()
            for column in month_columns
        ]
        load_cols = [group_by, *time_cols, *month_supplement_columns]
    else:
        time_cols = _month_columns(columns_all)
        load_cols = [group_by] + time_cols

    if len(load_cols) <= 1:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    df = repo.load_slice(columns=load_cols, filters=filters, limit=None, offset=0)
    if df.empty or group_by not in df.columns:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    df[group_by] = df[group_by].astype(str).fillna("未标注").str.strip()
    for tc in load_cols[1:]:
        if tc in df.columns:
            df[tc] = pd.to_numeric(df[tc], errors="coerce").fillna(0)

    if normalized_grain == "year":
        df, effective_year_columns = _prepare_effective_year_value_columns(
            df,
            supplemental_year_groups,
        )
        time_labels = list(effective_year_columns.keys())
        value_columns = [effective_year_columns[label] for label in time_labels]
    else:
        time_labels = time_cols
        value_columns = time_cols

    if not value_columns:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    time_labels = _filter_time_labels(time_labels, time_range)
    if normalized_grain == "year":
        value_columns = [
            effective_year_columns[label]
            for label in time_labels
            if label in effective_year_columns
        ]
    else:
        value_columns = [label for label in time_labels if label in df.columns]
    if not value_columns:
        return {"grain": normalized_grain, "rows": 0, "items": []}

    # Determine top-N series by total sales
    series_totals = (
        df.groupby(group_by)[value_columns]
        .sum()
        .sum(axis=1)
        .sort_values(ascending=False)
    )
    top_series = list(series_totals.head(max(1, int(top_n))).index)

    others_detail: list[dict] = []
    if include_others and len(series_totals) > len(top_series):
        df["_series"] = df[group_by].apply(lambda x: x if x in top_series else "其他")
        # Build others breakdown: each group rolled into "其他"
        others_names = [n for n in series_totals.index if n not in top_series]
        grand_total = float(series_totals.sum()) or 1.0
        for name in others_names:
            total_sales = float(series_totals[name])
            others_detail.append({
                "name": str(name),
                "sales": total_sales,
                "share": round(total_sales / grand_total * 100, 2),
            })
    else:
        df = df[df[group_by].isin(top_series)]
        df["_series"] = df[group_by]

    grouped = df.groupby("_series")[value_columns].sum()
    ordered = [s for s in top_series if s in grouped.index]
    if include_others and "其他" in grouped.index:
        ordered.append("其他")
    grouped = grouped.reindex(ordered)
    items = []
    for series_name, row in grouped.iterrows():
        if normalized_grain == "year":
            for time_label in time_labels:
                source_column = effective_year_columns.get(time_label)
                if source_column is None:
                    continue
                items.append({
                    "time": time_label,
                    "value": float(row.get(source_column, 0)),
                    "series": str(series_name),
                })
            continue
        for tc in time_labels:
            items.append({
                "time": tc,
                "value": float(row.get(tc, 0)),
                "series": str(series_name),
            })

    result: dict = {"grain": normalized_grain, "rows": len(items), "items": items}
    if others_detail:
        result["others_detail"] = others_detail
    return result


# ── Data freshness ──────────────────────────────────────────────
def get_data_freshness() -> list[dict[str, object]]:
    return repo.country_data_freshness()


# ── Overview ────────────────────────────────────────────────────
def query_overview(
    filters: dict[str, list[str]],
    prefer_precomputed: bool,
    top_n: int,
) -> dict:
    cache_key = (
        _normalize_query_cache_filters(filters),
        bool(prefer_precomputed),
        max(1, int(top_n)),
    )
    dataset_token = repo.current_dataset_token()
    now = time.monotonic()
    cached = _overview_cache.get(cache_key)
    if cached is not None:
        cached_at, cached_token, cached_result = cached
        if (
            cached_token == dataset_token
            and (now - cached_at) < _OVERVIEW_CACHE_TTL_SECONDS
        ):
            return cached_result

    with _overview_cache_lock:
        now = time.monotonic()
        dataset_token = repo.current_dataset_token()
        cached = _overview_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_token, cached_result = cached
            if (
                cached_token == dataset_token
                and (now - cached_at) < _OVERVIEW_CACHE_TTL_SECONDS
            ):
                return cached_result

        result = _query_overview_impl(
            filters=filters,
            prefer_precomputed=prefer_precomputed,
            top_n=top_n,
        )
        _overview_cache[cache_key] = (now, dataset_token, result)
        if len(_overview_cache) > 32:
            oldest_key = min(
                _overview_cache,
                key=lambda key: _overview_cache[key][0],
            )
            _overview_cache.pop(oldest_key, None)
        return result


def _query_overview_impl(
    filters: dict[str, list[str]],
    prefer_precomputed: bool,
    top_n: int,
) -> dict:
    columns = repo.list_columns()
    country_col = _resolve_existing_column(COUNTRY_CANDIDATES, columns)
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    make_col = _resolve_existing_column(MAKE_CANDIDATES, columns)
    model_col = _resolve_existing_column(MODEL_CANDIDATES, columns)
    version_col = _resolve_existing_column(VERSION_CANDIDATES, columns)

    total_rows = repo.count_rows(filters)
    country_count = repo.count_distinct(country_col, filters) if country_col else 0
    brand_count = repo.count_distinct(make_col, filters) if make_col else 0
    model_count = repo.count_distinct(model_col, filters) if model_col else 0
    version_count = repo.count_distinct(version_col, filters) if version_col else 0

    kpi_items = {
        "totalRows": int(total_rows),
        "countryCount": int(country_count),
        "brandCount": int(brand_count),
        "modelCount": int(model_count),
        "versionCount": int(version_count),
    }

    year_cols = _year_columns(columns)
    supplemental_year_groups = _supplemental_month_year_groups(columns)
    year_load_columns = [*year_cols]
    for month_columns in supplemental_year_groups.values():
        year_load_columns.extend(month_columns)
    if year_load_columns:
        df_years = repo.load_slice(columns=year_load_columns, filters=filters, limit=200_000, offset=0)
        if not df_years.empty:
            df_years, effective_year_columns = _prepare_effective_year_value_columns(
                df_years,
                supplemental_year_groups,
            )
            total_sales = 0.0
            for source_column in effective_year_columns.values():
                if source_column in df_years.columns:
                    total_sales += float(
                        pd.to_numeric(
                            df_years[source_column],
                            errors="coerce",
                        ).sum(skipna=True)
                    )
            kpi_items["cumulativeSales"] = total_sales

    if msrp_col:
        df = repo.load_slice(columns=[msrp_col], filters=filters, limit=5000, offset=0)
        if not df.empty and msrp_col in df.columns:
            numeric = pd.to_numeric(df[msrp_col], errors="coerce")
            kpi_items["avgMsrp"] = float(numeric.mean(skipna=True))

    month_series = query_time_series(filters=filters, grain="month", top_n=top_n)
    year_series = query_time_series(filters=filters, grain="year", top_n=max(5, min(30, top_n)))

    route = "raw"
    if prefer_precomputed and sum(1 for vals in filters.values() if vals) <= 1:
        pre = repo.load_precomputed("country")
        if not pre.empty:
            route = "precomputed"
    elif total_rows > 0:
        route = "dynamic-aggregate"

    return {
        "route": route,
        "kpis": kpi_items,
        "monthSeries": month_series["items"],
        "yearSeries": year_series["items"],
    }


# ── Detail ──────────────────────────────────────────────────────
def query_detail(
    filters: dict[str, list[str]],
    columns: list[str],
    page: int,
    page_size: int,
    exclude_zero_sales: bool = False,
) -> dict:
    page_num = max(1, int(page))
    size = max(1, int(page_size))
    offset = (page_num - 1) * size
    selected_columns = columns or None
    if exclude_zero_sales:
        all_rows = _load_excluding_zero_sales(filters, selected_columns)
        total = len(all_rows)
        rows = all_rows.iloc[offset:offset + size]
    else:
        rows = repo.load_slice(columns=selected_columns, filters=filters, limit=size, offset=offset)
        total = repo.count_rows(filters)
    return {"page": page_num, "pageSize": size, "total": int(total), "items": rows.to_dict(orient="records")}


def export_detail_csv(
    filters: dict[str, list[str]],
    columns: list[str],
    max_rows: int,
    exclude_zero_sales: bool = False,
) -> bytes:
    capped_rows = max(1, int(max_rows))
    selected_columns = columns or None
    if exclude_zero_sales:
        rows = _load_excluding_zero_sales(filters, selected_columns).head(capped_rows)
    else:
        rows = repo.load_slice(columns=selected_columns, filters=filters, limit=capped_rows, offset=0)
    stream = io.StringIO()
    rows.to_csv(stream, index=False)
    return stream.getvalue().encode("utf-8-sig")


# ── Simple aggregation helper ──────────────────────────────────
def _aggregate_count(
    filters: dict[str, list[str]],
    target_column: str,
    top_n: int,
) -> list[dict]:
    frame = repo.load_slice(columns=[target_column], filters=filters, limit=200_000, offset=0)
    if frame.empty or target_column not in frame.columns:
        return []
    grouped = frame[target_column].astype(str).value_counts(dropna=False).head(max(1, int(top_n)))
    return [{"label": str(label), "value": int(value)} for label, value in grouped.items()]


# ── Vehicle frame builder ──────────────────────────────────────
def _build_vehicle_frame(
    filters: dict[str, list[str]],
    *,
    include_year_columns: bool = False,
    sales_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load a vehicle-level frame with Make, Model, Segment, Powertrain, Length, MSRP, Sales."""
    columns = repo.list_columns()
    make_col = _resolve_existing_column(MAKE_CANDIDATES, columns)
    model_col = _resolve_existing_column(MODEL_CANDIDATES, columns)
    version_col = _resolve_existing_column(VERSION_CANDIDATES, columns)
    segment_col = _resolve_existing_column(SEGMENT_CANDIDATES, columns)
    powertrain_col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
    length_col = _resolve_existing_column(LENGTH_CANDIDATES, columns)
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    year_cols = _year_columns(columns)
    supplemental_year_groups: dict[str, list[str]] = {}
    year_load_columns = [*year_cols]
    if include_year_columns:
        supplemental_year_groups = _supplemental_month_year_groups(columns)
        for month_columns in supplemental_year_groups.values():
            year_load_columns.extend(month_columns)

    explicit_sales_scope = sales_columns is not None
    requested_sales_columns = [column for column in (sales_columns or []) if column]
    if explicit_sales_scope and not requested_sales_columns:
        return pd.DataFrame()
    needed = (
        [
            c
            for c in [
                make_col,
                model_col,
                version_col,
                segment_col,
                powertrain_col,
                length_col,
                msrp_col,
            ]
            if c
        ]
        + year_load_columns
        + requested_sales_columns
    )
    needed = list(dict.fromkeys(needed))
    if not needed:
        return pd.DataFrame()

    df = repo.load_slice(columns=needed, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return df

    effective_year_columns: dict[str, str] = {}
    if include_year_columns and year_load_columns:
        df, effective_year_columns = _prepare_effective_year_value_columns(
            df,
            supplemental_year_groups,
        )
    elif year_cols:
        effective_year_columns = {
            year_label: year_label
            for year_label in year_cols
            if year_label in df.columns
        }

    out = pd.DataFrame()
    if make_col and make_col in df.columns:
        out["Brand"] = df[make_col].astype(str).str.strip()
    if model_col and model_col in df.columns:
        out["Model"] = df[model_col].astype(str).str.strip()
    if version_col and version_col in df.columns:
        out["Version"] = df[version_col].astype(str).str.strip()
    if segment_col and segment_col in df.columns:
        out["Segment"] = df[segment_col].astype(str).str.strip()
    if powertrain_col and powertrain_col in df.columns:
        out["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    if length_col and length_col in df.columns:
        out["Length"] = pd.to_numeric(df[length_col], errors="coerce")
    if msrp_col and msrp_col in df.columns:
        out["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    if include_year_columns:
        for year_label, source_column in effective_year_columns.items():
            if source_column in df.columns:
                out[year_label] = pd.to_numeric(df[source_column], errors="coerce").fillna(0.0)
    sales_source_columns = (
        requested_sales_columns
        or (list(effective_year_columns.values()) if include_year_columns else year_cols)
    )
    out["Sales"] = _sum_sales_columns(df, sales_source_columns)
    return out


# ── Advanced chart dispatcher ──────────────────────────────────
def query_advanced_chart(
    group: str,
    chart: str,
    filters: dict[str, list[str]],
    top_n: int,
    options: dict | None = None,
    time_range: dict[str, str] | None = None,
) -> dict:
    columns = repo.list_columns()
    ng = str(group).strip().lower()
    nc = str(chart).strip().lower()
    opts = dict(options or {})
    if time_range is not None:
        opts["time_range"] = time_range
    empty = {"group": group, "chart": chart, "rows": 0, "items": []}

    # ── Market structure ────────────────────────────────────────
    if ng == "market_structure":
        if nc == "powertrain_mix":
            col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
            if not col:
                return empty
            sales_columns = _resolve_sales_columns_from_options(columns, opts)
            if _has_explicit_time_range(opts) and not sales_columns:
                return {"group": group, "chart": chart, "rows": 0, "items": []}
            items = (
                _aggregate_sales(filters, col, top_n, sales_columns)
                if sales_columns else _aggregate_count(filters, col, top_n)
            )
            return {"group": group, "chart": chart, "rows": len(items), "items": items}

        if nc == "segment_share":
            col = _resolve_existing_column(SEGMENT_CANDIDATES, columns)
            if not col:
                return empty
            sales_columns = _resolve_sales_columns_from_options(columns, opts)
            if _has_explicit_time_range(opts) and not sales_columns:
                return {"group": group, "chart": chart, "rows": 0, "items": []}
            items = (
                _aggregate_sales(filters, col, top_n, sales_columns)
                if sales_columns else _aggregate_count(filters, col, top_n)
            )
            return {"group": group, "chart": chart, "rows": len(items), "items": items}

        if nc == "powertrain_bubble":
            return _chart_powertrain_bubble(filters, top_n, opts)

        if nc == "segment_share_by_length":
            return _chart_segment_share_by_length(filters, opts)

    # ── NEV analysis ────────────────────────────────────────────
    if ng == "nev_analysis":
        if nc == "nev_range_distribution":
            return _chart_nev_range_distribution(filters, top_n, opts)

        if nc == "nev_capacity_vs_msrp":
            return _chart_nev_capacity_vs_msrp(filters, top_n, opts)

    # ── Price value ─────────────────────────────────────────────
    if ng == "price_value":
        if nc == "top_makes":
            col = _resolve_existing_column(MAKE_CANDIDATES, columns)
            if not col:
                return empty
            items = _aggregate_count(filters, col, top_n)
            return {"group": group, "chart": chart, "rows": len(items), "items": items}

        if nc == "price_migration":
            return _chart_price_migration(filters, opts)

        if nc == "length_vs_price":
            return _chart_length_vs_price(filters, top_n, opts)

        if nc == "price_per_meter":
            return _chart_price_per_meter(filters, top_n, opts)

        if nc == "sales_vs_price":
            return _chart_sales_vs_price(filters, top_n, opts)

        if nc == "powertrain_vs_price":
            return _chart_powertrain_vs_price(filters, opts)

    # ── Cost analysis ───────────────────────────────────────────
    if ng == "cost_analysis":
        if nc == "estimated_tco":
            return _chart_estimated_tco(filters, top_n, opts)

    # ── Time insight ────────────────────────────────────────────
    if ng == "time_insight":
        if nc == "seasonality_heatmap":
            monthly = query_time_series(filters=filters, grain="month", top_n=120)["items"]
            normalized_time_range = _extract_time_range(opts.get("time_range"))
            allowed_labels = set(
                _filter_time_labels(
                    [str(row.get("time", "")).strip() for row in monthly],
                    opts.get("time_range"),
                )
            )
            matrix: list[dict] = []
            for row in monthly:
                time_key = str(row.get("time", "")).strip()
                if normalized_time_range is not None and time_key not in allowed_labels:
                    continue
                if len(time_key) < 8 or " " not in time_key:
                    continue
                year, month = time_key.split(" ", 1)
                matrix.append({"year": year, "month": month, "value": float(row.get("value", 0.0))})
            return {"group": group, "chart": chart, "rows": len(matrix), "items": matrix}

    return empty


# ── Chart: Powertrain Bubble ───────────────────────────────────
def _chart_powertrain_bubble(
    filters: dict[str, list[str]], top_n: int, opts: dict,
) -> dict:
    max_top_n = max(1, int(top_n))
    show_yoy = bool(opts.get("show_yoy"))
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), opts)
    vf = _build_vehicle_frame(
        filters,
        include_year_columns=show_yoy,
        sales_columns=sales_columns,
    )
    if vf.empty or "Powertrain" not in vf.columns:
        return {"group": "market_structure", "chart": "powertrain_bubble", "rows": 0, "items": []}

    vf = vf[vf["Powertrain"].isin(POWERTRAIN_DISPLAY_ORDER)]
    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[vf["Length"] > 0]
    vf = vf[vf["MSRP"] > 0]

    grain = str(opts.get("grain", "model")).strip().lower()
    use_version_grain = grain == "version" and "Version" in vf.columns

    warnings: list[str] = []
    year_options = sorted(
        [column for column in vf.columns if len(column) == 4 and column.isdigit()],
        key=int,
    )
    yoy_compare_year = str(opts.get("yoy_compare_year", "")).strip()
    yoy_base_year = ""
    if show_yoy:
        if len(year_options) < 2:
            show_yoy = False
            warnings.append("年度列不足两年，已自动关闭 YoY。")
        else:
            if yoy_compare_year not in year_options:
                yoy_compare_year = year_options[-1]
            compare_index = year_options.index(yoy_compare_year)
            if compare_index == 0:
                show_yoy = False
                warnings.append("所选 YoY 年份缺少上一年基准，已自动关闭 YoY。")
            else:
                yoy_base_year = year_options[compare_index - 1]

    group_top_n = bool(opts.get("group_top_n"))
    group_dimension = str(opts.get("group_dimension", "segment")).strip().lower()
    group_field = "Segment" if group_dimension == "segment" else "Powertrain"
    group_dimension_label = "细分市场" if group_field == "Segment" else "动总规整"
    raw_group_values = opts.get("group_values", [])
    requested_group_values = [
        str(value).strip()
        for value in raw_group_values
        if str(value).strip()
    ] if isinstance(raw_group_values, list) else []
    raw_group_top_n_map = opts.get("group_top_n_map", {})
    group_top_n_map: dict[str, int] = {}
    if isinstance(raw_group_top_n_map, dict):
        for key, value in raw_group_top_n_map.items():
            try:
                group_top_n_map[str(key).strip()] = max(1, min(300, int(value)))
            except (TypeError, ValueError):
                continue

    available_group_values: list[str] = []
    selected_group_values: list[str] = []
    grouped_top_n_applied = False
    if group_top_n:
        if group_field not in vf.columns:
            warnings.append(f"当前数据不包含{group_dimension_label}字段，已跳过分组 TopN。")
        else:
            grouped_rank_df = (
                vf.groupby([group_field, "Model"], as_index=False)["Sales"]
                .sum()
                .sort_values("Sales", ascending=False)
            )
            available_group_values = grouped_rank_df[group_field].astype(str).drop_duplicates().tolist()
            available_group_set = set(available_group_values)
            selected_group_values = [
                value for value in requested_group_values
                if value in available_group_set
            ]
            if selected_group_values:
                keep_chunks: list[pd.DataFrame] = []
                for grouped_value in selected_group_values:
                    group_topn = int(group_top_n_map.get(grouped_value, max_top_n))
                    group_rank = grouped_rank_df[grouped_rank_df[group_field] == grouped_value]
                    if group_rank.empty:
                        continue
                    keep_chunks.append(
                        group_rank.head(group_topn)[[group_field, "Model"]]
                    )
                if keep_chunks:
                    keep_pairs = pd.concat(keep_chunks, ignore_index=True).drop_duplicates()
                    vf = vf.merge(keep_pairs, on=[group_field, "Model"], how="inner")
                    grouped_top_n_applied = True
            else:
                warnings.append("已启用分组 TopN，但未选中有效分组。")

    if show_yoy and yoy_compare_year and yoy_base_year:
        vf["SalesCurrent"] = pd.to_numeric(vf[yoy_compare_year], errors="coerce").fillna(0.0)
        vf["SalesBase"] = pd.to_numeric(vf[yoy_base_year], errors="coerce").fillna(0.0)
    else:
        vf["SalesCurrent"] = vf["Sales"]
        vf["SalesBase"] = vf["Sales"]

    group_cols = ["Model", "Powertrain"]
    if use_version_grain:
        group_cols.insert(1, "Version")
    if "Brand" in vf.columns:
        group_cols.append("Brand")
    if grouped_top_n_applied and group_field == "Segment" and "Segment" in vf.columns:
        group_cols.append("Segment")

    grouped = vf.groupby(group_cols, dropna=False)
    agg = grouped.agg(
        Length=("Length", "median"),
        MSRP=("MSRP", "median"),
        MsrpMin=("MSRP", "min"),
        MsrpMax=("MSRP", "max"),
        Sales=("Sales", "sum"),
        SalesCurrent=("SalesCurrent", "sum"),
        SalesBase=("SalesBase", "sum"),
    ).reset_index()

    if use_version_grain:
        agg["DisplayName"] = (
            agg["Model"].astype(str).str.strip()
            + " / "
            + agg["Version"].astype(str).str.strip()
        ).str.strip(" /")
        agg["VariantCount"] = 1
    else:
        agg["DisplayName"] = agg["Model"].astype(str).str.strip()
        if "Version" in vf.columns:
            variant_counts = grouped["Version"].nunique().reset_index(name="VariantCount")
        else:
            variant_counts = grouped.size().reset_index(name="VariantCount")
        agg = agg.merge(variant_counts, on=group_cols, how="left")

    agg["BubbleGrain"] = "version" if use_version_grain else "model"

    if "Segment" in vf.columns and "Segment" not in group_cols:
        segment_map = grouped["Segment"].agg(
            lambda series: str(series.mode().iat[0]) if not series.mode().empty else str(series.iloc[0])
        ).reset_index(name="Segment")
        agg = agg.merge(segment_map, on=group_cols, how="left")

    if show_yoy and yoy_compare_year and yoy_base_year:
        base_sales = pd.to_numeric(agg["SalesBase"], errors="coerce")
        current_sales = pd.to_numeric(agg["SalesCurrent"], errors="coerce")
        yoy_ratio = (current_sales - base_sales).div(base_sales.where(base_sales != 0))
        yoy_ratio = pd.to_numeric(yoy_ratio, errors="coerce").replace([np.inf, -np.inf], np.nan)
        agg["YoYPct"] = (yoy_ratio.fillna(0.0).clip(-0.8, 3.0) * 100).round(2)
    else:
        agg["YoYPct"] = 0.0

    agg = agg.sort_values("Sales", ascending=False)
    if not grouped_top_n_applied:
        agg = agg.head(max_top_n)

    items = agg.to_dict(orient="records")
    return {
        "group": "market_structure",
        "chart": "powertrain_bubble",
        "rows": len(items),
        "items": items,
        "meta": {
            "yoyEnabled": bool(show_yoy and yoy_compare_year and yoy_base_year),
            "yoyCompareYear": yoy_compare_year,
            "yoyBaseYear": yoy_base_year,
            "availableYears": year_options,
            "groupTopNApplied": grouped_top_n_applied,
            "groupDimension": group_dimension,
            "groupDimensionLabel": group_dimension_label,
            "groupValues": selected_group_values,
            "availableGroupValues": available_group_values,
            "warnings": warnings,
        },
    }


# ── Chart: Segment Share by Length ─────────────────────────────
def _chart_segment_share_by_length(
    filters: dict[str, list[str]], opts: dict,
) -> dict:
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), opts)
    vf = _build_vehicle_frame(filters, sales_columns=sales_columns)
    if vf.empty or "Length" not in vf.columns or "Segment" not in vf.columns:
        return {"group": "market_structure", "chart": "segment_share_by_length", "rows": 0, "items": []}

    band_size = int(opts.get("band_size", 100))
    vf = vf.dropna(subset=["Length"])
    vf = vf[vf["Length"] > 0]
    vf["LengthBand"] = _make_length_bands(vf["Length"], band_size)

    grouped = vf.groupby(["LengthBand", "Segment"], dropna=False)["Sales"].sum().reset_index()
    grouped = grouped.sort_values("LengthBand")
    items = grouped.to_dict(orient="records")
    return {"group": "market_structure", "chart": "segment_share_by_length", "rows": len(items), "items": items}


# ── Chart: NEV Range Distribution ──────────────────────────────
def _chart_nev_range_distribution(
    filters: dict[str, list[str]], top_n: int, opts: dict,
) -> dict:
    columns = repo.list_columns()
    range_col = _resolve_existing_column(BATTERY_RANGE_CANDIDATES, columns)
    powertrain_col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
    model_col = _resolve_existing_column(MODEL_CANDIDATES, columns)
    make_col = _resolve_existing_column(MAKE_CANDIDATES, columns)
    if not range_col or not powertrain_col:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": []}

    year_cols = _year_columns(columns)
    normalized_time_range = _extract_time_range(opts.get("time_range"))
    sales_scope_columns = _resolve_sales_columns_from_options(columns, opts)
    if normalized_time_range is not None and not sales_scope_columns:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": []}
    boundary_start_columns = (
        _sales_columns_for_time_label(columns, normalized_time_range[0])
        if normalized_time_range is not None
        else []
    )
    boundary_end_columns = (
        _sales_columns_for_time_label(columns, normalized_time_range[1])
        if normalized_time_range is not None
        else []
    )
    load_cols = list(
        dict.fromkeys(
            [c for c in [powertrain_col, range_col, model_col, make_col] if c]
            + year_cols
            + sales_scope_columns
            + boundary_start_columns
            + boundary_end_columns
        )
    )
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": []}

    df["BatteryRange"] = pd.to_numeric(df[range_col], errors="coerce")
    df["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    if model_col and model_col in df.columns:
        df["Model"] = df[model_col].astype(str).str.strip()
    if make_col and make_col in df.columns:
        df["Brand"] = df[make_col].astype(str).str.strip()

    for yc in year_cols:
        if yc in df.columns:
            df[yc] = pd.to_numeric(df[yc], errors="coerce").fillna(0.0)
    df["SalesWindow"] = _sum_sales_columns(
        df,
        sales_scope_columns or year_cols,
    )

    # Filter to NEV powertrains
    selected_pt = opts.get("powertrains", ["BEV", "PHEV"])
    if isinstance(selected_pt, str):
        selected_pt = [selected_pt]
    df = df[df["Powertrain"].isin(selected_pt)]
    df = df.dropna(subset=["BatteryRange"])
    df = df[df["BatteryRange"] > 0]
    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": []}

    top_n_enabled = bool(opts.get("top_n_enabled", True))
    axis_max = int(opts.get("axis_max", 1000))
    range_step = int(opts.get("range_step", 50))
    metric_mode = str(opts.get("metric_mode", "window_sales")).strip().lower()
    stack_by_model = bool(opts.get("stack_by_model", False)) and "Model" in df.columns
    facet_brand = bool(opts.get("facet_brand", False)) and "Brand" in df.columns
    max_brand_facets = int(opts.get("max_brand_facets", 4))
    warnings: list[str] = []

    start_year_label = normalized_time_range[0] if normalized_time_range is not None else (year_cols[0] if year_cols else None)
    end_year_label = normalized_time_range[1] if normalized_time_range is not None else (year_cols[-1] if year_cols else None)
    start_growth_columns = boundary_start_columns
    end_growth_columns = boundary_end_columns
    if normalized_time_range is None:
        start_growth_columns = [start_year_label] if start_year_label in df.columns else []
        end_growth_columns = [end_year_label] if end_year_label in df.columns else []
    growth_ready = (
        start_year_label is not None
        and end_year_label is not None
        and start_year_label != end_year_label
        and len(start_growth_columns) > 0
        and len(end_growth_columns) > 0
    )
    if growth_ready:
        df["SalesWindowStartYear"] = _sum_sales_columns(df, start_growth_columns)
        df["SalesWindowEndYear"] = _sum_sales_columns(df, end_growth_columns)
        df["GrowthWindow"] = df["SalesWindowEndYear"] - df["SalesWindowStartYear"]
        df["GrowthAbsWindow"] = df["GrowthWindow"].abs()
    else:
        df["SalesWindowStartYear"] = 0.0
        df["SalesWindowEndYear"] = 0.0
        df["GrowthWindow"] = 0.0
        df["GrowthAbsWindow"] = 0.0

    if metric_mode == "net_change" and not growth_ready:
        metric_mode = "window_sales"
        warnings.append("当前数据不足两个年份，已回退到当前时间窗销量口径。")

    metric_column = "GrowthWindow" if metric_mode == "net_change" else "SalesWindow"
    ranking_column = "GrowthAbsWindow" if metric_mode == "net_change" else "SalesWindow"
    metric_title = (
        f"销量净变化（{end_year_label}-{start_year_label}）"
        if metric_mode == "net_change" and growth_ready and start_year_label and end_year_label
        else ("销量净变化（末年-首年）" if metric_mode == "net_change" else "销量")
    )

    df = df[df["BatteryRange"].between(0, axis_max)]
    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": [], "meta": {"warnings": ["当前筛选下续航数据超出上限或为空。"]}}

    if top_n_enabled and "Model" in df.columns:
        model_rank = df.groupby("Model", as_index=False)[ranking_column].sum().sort_values(ranking_column, ascending=False)
        top_models = set(model_rank.head(max(1, int(top_n)))["Model"])
        df = df[df["Model"].isin(top_models)]

    selected_brands: list[str] = []
    if facet_brand and "Brand" in df.columns:
        brand_rank = df.groupby("Brand", as_index=False)[ranking_column].sum().sort_values(ranking_column, ascending=False)
        selected_brands = brand_rank.head(max(1, int(max_brand_facets)))["Brand"].tolist()
        df = df[df["Brand"].isin(selected_brands)]

    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": 0, "items": []}

    df["RangeBand"] = (df["BatteryRange"] // range_step * range_step).astype(int)
    df["RangeBand"] = df["RangeBand"].clip(lower=0, upper=max(0, axis_max - range_step))
    df["RangeBandLabel"] = df["RangeBand"].map(lambda start: f"{int(start)}-{int(min(start + range_step - 1, axis_max))}")

    stack_key = "Model" if stack_by_model else "Powertrain"
    group_fields = ["RangeBand", "RangeBandLabel", stack_key]
    if facet_brand and "Brand" in df.columns:
        group_fields.insert(0, "Brand")

    grouped = df.groupby(group_fields, dropna=False)[metric_column].sum().reset_index()
    grouped = grouped.rename(columns={metric_column: "Value"})
    grouped = grouped.sort_values("RangeBand")
    items = grouped.to_dict(orient="records")

    meta: dict[str, object] = {
        "metricMode": metric_mode,
        "metricField": "Value",
        "metricTitle": metric_title,
        "rangeColumn": range_col,
        "stackKey": stack_key,
        "splitByBrand": facet_brand,
        "brands": selected_brands,
        "rangeStep": range_step,
        "axisMax": axis_max,
        "warnings": warnings,
    }

    if metric_mode == "net_change":
        net_change_total = float(df["GrowthWindow"].sum())
        abs_change_total = float(df["GrowthAbsWindow"].sum())
        offset_ratio = 1.0 - abs(net_change_total) / abs_change_total if abs_change_total > 0 else 0.0

        def _weighted_avg_range(sales_col: str) -> float | None:
            weight_sum = float(df[sales_col].sum())
            if weight_sum <= 0:
                return None
            return float((df["BatteryRange"] * df[sales_col]).sum() / weight_sum)

        weighted_range_start = _weighted_avg_range("SalesWindowStartYear")
        weighted_range_end = _weighted_avg_range("SalesWindowEndYear")
        weighted_range_delta = (
            float(weighted_range_end - weighted_range_start)
            if weighted_range_start is not None and weighted_range_end is not None
            else None
        )

        annual_sales = [{"year": yc, "sales": float(df[yc].sum())} for yc in year_cols if yc in df.columns]
        powertrain_summary = (
            df.groupby("Powertrain", as_index=False)[["GrowthWindow", "GrowthAbsWindow"]]
            .sum()
            .sort_values("GrowthAbsWindow", ascending=False)
            .to_dict(orient="records")
        )
        bucket_summary = (
            df.groupby(["RangeBand", "RangeBandLabel"], as_index=False)[["SalesWindowStartYear", "SalesWindowEndYear", "GrowthWindow"]]
            .sum()
            .sort_values("RangeBand")
        )
        if net_change_total != 0:
            bucket_summary["NetShare"] = bucket_summary["GrowthWindow"] / net_change_total
        else:
            bucket_summary["NetShare"] = 0.0
        bucket_positive = (
            bucket_summary.sort_values("GrowthWindow", ascending=False)
            .head(3)
            .copy()
        )
        bucket_negative = (
            bucket_summary.sort_values("GrowthWindow", ascending=True)
            .head(3)
            .copy()
        )
        model_movers = pd.DataFrame()
        model_gains = pd.DataFrame()
        model_declines = pd.DataFrame()
        if "Model" in df.columns:
            model_movers = (
                df.groupby("Model", as_index=False)[["GrowthWindow", "GrowthAbsWindow", "SalesWindowStartYear", "SalesWindowEndYear"]]
                .sum()
                .sort_values("GrowthAbsWindow", ascending=False)
            )
        top_model_limit = int(min(10, len(model_movers))) if not model_movers.empty else 0
        if top_model_limit > 0:
            model_gains = (
                model_movers.sort_values("GrowthWindow", ascending=False)
                .head(top_model_limit)
                .copy()
            )
            model_declines = (
                model_movers.sort_values("GrowthWindow", ascending=True)
                .head(top_model_limit)
                .copy()
            )
        top_model_abs_share = (
            float(model_movers.head(top_model_limit)["GrowthAbsWindow"].sum() / abs_change_total)
            if top_model_limit > 0 and abs_change_total > 0
            else None
        )
        if top_model_abs_share is not None and top_model_abs_share >= 0.70:
            warnings.append(f"Top{top_model_limit} Model 贡献了 {top_model_abs_share:.1%} 的 |净变化|，集中度较高。")
        if offset_ratio >= 0.85:
            warnings.append("结构对冲率较高，建议结合分桶与 Top 车型明细查看。")

        meta.update({
            "growthSpanLabel": f"{end_year_label}-{start_year_label}" if growth_ready and start_year_label and end_year_label else "末年-首年",
            "kpis": {
                "netChangeTotal": net_change_total,
                "absChangeTotal": abs_change_total,
                "offsetRatio": offset_ratio,
                "weightedRangeEnd": weighted_range_end,
                "weightedRangeDelta": weighted_range_delta,
            },
            "annualSales": annual_sales,
            "powertrainSummary": powertrain_summary,
            "bucketSummary": bucket_summary.to_dict(orient="records"),
            "bucketPositive": bucket_positive.to_dict(orient="records"),
            "bucketNegative": bucket_negative.to_dict(orient="records"),
            "modelMovers": model_movers.head(top_model_limit).to_dict(orient="records") if top_model_limit > 0 else [],
            "modelGains": model_gains.to_dict(orient="records") if top_model_limit > 0 else [],
            "modelDeclines": model_declines.to_dict(orient="records") if top_model_limit > 0 else [],
            "topModelLimit": top_model_limit,
            "topModelAbsShare": top_model_abs_share,
            "warnings": warnings,
        })

    return {"group": "nev_analysis", "chart": "nev_range_distribution", "rows": len(items), "items": items, "meta": meta}


# ── Chart: NEV Capacity vs MSRP ───────────────────────────────
def _chart_nev_capacity_vs_msrp(
    filters: dict[str, list[str]], top_n: int, opts: dict,
) -> dict:
    columns = repo.list_columns()
    cap_col = _resolve_existing_column(BATTERY_CAPACITY_CANDIDATES, columns)
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    powertrain_col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
    model_col = _resolve_existing_column(MODEL_CANDIDATES, columns)
    make_col = _resolve_existing_column(MAKE_CANDIDATES, columns)
    if not cap_col or not msrp_col or not powertrain_col:
        return {"group": "nev_analysis", "chart": "nev_capacity_vs_msrp", "rows": 0, "items": []}

    year_cols = _year_columns(columns)
    sales_scope_columns = _resolve_sales_columns_from_options(columns, opts)
    if _has_explicit_time_range(opts) and not sales_scope_columns:
        return {"group": "nev_analysis", "chart": "nev_capacity_vs_msrp", "rows": 0, "items": []}
    load_cols = list(
        dict.fromkeys(
            [c for c in [cap_col, msrp_col, powertrain_col, model_col, make_col] if c]
            + year_cols
            + sales_scope_columns
        )
    )
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_capacity_vs_msrp", "rows": 0, "items": []}

    df["BatteryCapacity"] = pd.to_numeric(df[cap_col], errors="coerce")
    df["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    df["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    df["Sales"] = _sum_sales_columns(df, sales_scope_columns or year_cols)
    if model_col and model_col in df.columns:
        df["Model"] = df[model_col].astype(str).str.strip()
    if make_col and make_col in df.columns:
        df["Brand"] = df[make_col].astype(str).str.strip()

    selected_pt = opts.get("powertrains", ["BEV", "PHEV"])
    if isinstance(selected_pt, str):
        selected_pt = [selected_pt]
    df = df[df["Powertrain"].isin(selected_pt)]
    df = df.dropna(subset=["BatteryCapacity", "MSRP"])
    df = df[(df["BatteryCapacity"] > 0) & (df["MSRP"] > 0)]

    group_cols = [c for c in ["Model", "Brand", "Powertrain"] if c in df.columns]
    agg = df.groupby(group_cols, dropna=False).agg(
        BatteryCapacity=("BatteryCapacity", "median"),
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    ).reset_index()
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))
    items = agg.to_dict(orient="records")
    return {"group": "nev_analysis", "chart": "nev_capacity_vs_msrp", "rows": len(items), "items": items}


# ── Chart: Price Migration ─────────────────────────────────────
def _chart_price_migration(
    filters: dict[str, list[str]], opts: dict,
) -> dict:
    columns = repo.list_columns()
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    if not msrp_col:
        return {"group": "price_value", "chart": "price_migration", "rows": 0, "items": []}

    year_cols = _year_columns(columns)
    sales_scope_columns = _resolve_sales_columns_from_options(columns, opts)
    if _has_explicit_time_range(opts) and not sales_scope_columns:
        return {"group": "price_value", "chart": "price_migration", "rows": 0, "items": []}
    if not year_cols and not sales_scope_columns:
        return {"group": "price_value", "chart": "price_migration", "rows": 0, "items": []}

    load_cols = list(dict.fromkeys([msrp_col] + year_cols + sales_scope_columns))
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "price_value", "chart": "price_migration", "rows": 0, "items": []}

    band_size = int(opts.get("band_size", 1000))
    df["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    df = df.dropna(subset=["MSRP"])
    df = df[df["MSRP"] > 0]
    df["PriceBand"] = _make_price_bands(df["MSRP"], band_size)

    # For each year column, aggregate sales by price band
    items = []
    for yc in (sales_scope_columns or year_cols):
        if yc not in df.columns:
            continue
        df[yc] = pd.to_numeric(df[yc], errors="coerce").fillna(0)
        band_sales = df.groupby("PriceBand")[yc].sum().reset_index()
        for _, row in band_sales.iterrows():
            if float(row[yc]) > 0:
                items.append({
                    "priceBand": int(row["PriceBand"]),
                    "year": yc,
                    "sales": float(row[yc]),
                })

    items.sort(key=lambda x: (x["priceBand"], x["year"]))
    return {"group": "price_value", "chart": "price_migration", "rows": len(items), "items": items}


# ── Chart: Length vs Price Map ─────────────────────────────────
def _chart_length_vs_price(
    filters: dict[str, list[str]], top_n: int, opts: dict | None = None,
) -> dict:
    scope_opts = opts or {}
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), scope_opts)
    vf = _build_vehicle_frame(filters, sales_columns=sales_columns)
    if vf.empty or "Length" not in vf.columns or "MSRP" not in vf.columns:
        return {"group": "price_value", "chart": "length_vs_price", "rows": 0, "items": []}

    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[(vf["Length"] > 0) & (vf["MSRP"] > 0)]

    group_cols = [c for c in ["Brand", "Model", "Segment", "Powertrain"] if c in vf.columns]
    if not group_cols:
        return {"group": "price_value", "chart": "length_vs_price", "rows": 0, "items": []}

    agg = vf.groupby(group_cols, dropna=False).agg(
        Length=("Length", "median"),
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    ).reset_index()
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))
    items = agg.to_dict(orient="records")
    return {"group": "price_value", "chart": "length_vs_price", "rows": len(items), "items": items}


# ── Chart: Price Per Meter vs Sales ────────────────────────────
def _chart_price_per_meter(
    filters: dict[str, list[str]], top_n: int, opts: dict | None = None,
) -> dict:
    scope_opts = opts or {}
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), scope_opts)
    vf = _build_vehicle_frame(filters, sales_columns=sales_columns)
    if vf.empty or "Length" not in vf.columns or "MSRP" not in vf.columns:
        return {"group": "price_value", "chart": "price_per_meter", "rows": 0, "items": []}

    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[(vf["Length"] > 0) & (vf["MSRP"] > 0)]
    vf["LengthMeter"] = vf["Length"] / 1000.0
    vf["PricePerMeter"] = vf["MSRP"] / vf["LengthMeter"]

    group_cols = [c for c in ["Brand", "Model"] if c in vf.columns]
    if not group_cols:
        return {"group": "price_value", "chart": "price_per_meter", "rows": 0, "items": []}

    agg = vf.groupby(group_cols, dropna=False).agg(
        PricePerMeter=("PricePerMeter", "median"),
        Sales=("Sales", "sum"),
    ).reset_index()
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))
    items = agg.to_dict(orient="records")
    return {"group": "price_value", "chart": "price_per_meter", "rows": len(items), "items": items}


# ── Chart: Sales vs Price Scatter ──────────────────────────────
def _chart_sales_vs_price(
    filters: dict[str, list[str]], top_n: int, opts: dict | None = None,
) -> dict:
    scope_opts = opts or {}
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), scope_opts)
    vf = _build_vehicle_frame(filters, sales_columns=sales_columns)
    if vf.empty or "MSRP" not in vf.columns:
        return {"group": "price_value", "chart": "sales_vs_price", "rows": 0, "items": []}

    vf = vf.dropna(subset=["MSRP"])
    vf = vf[vf["MSRP"] > 0]

    group_cols = [c for c in ["Segment", "Brand", "Model"] if c in vf.columns]
    if not group_cols:
        return {"group": "price_value", "chart": "sales_vs_price", "rows": 0, "items": []}

    agg = vf.groupby(group_cols, dropna=False).agg(
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    ).reset_index()

    if "Segment" in agg.columns:
        seg_total = agg.groupby("Segment")["Sales"].transform("sum")
        agg["SegmentSharePct"] = (agg["Sales"] / seg_total.replace(0, 1) * 100).round(2)
    else:
        agg["SegmentSharePct"] = 0

    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))
    items = agg.to_dict(orient="records")
    return {"group": "price_value", "chart": "sales_vs_price", "rows": len(items), "items": items}


# ── Chart: Powertrain vs Price ─────────────────────────────────
def _chart_powertrain_vs_price(
    filters: dict[str, list[str]], opts: dict,
) -> dict:
    columns = repo.list_columns()
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    powertrain_col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
    if not msrp_col or not powertrain_col:
        return {"group": "price_value", "chart": "powertrain_vs_price", "rows": 0, "items": []}

    year_cols = _year_columns(columns)
    sales_scope_columns = _resolve_sales_columns_from_options(columns, opts)
    if _has_explicit_time_range(opts) and not sales_scope_columns:
        return {"group": "price_value", "chart": "powertrain_vs_price", "rows": 0, "items": []}
    load_cols = list(
        dict.fromkeys([msrp_col, powertrain_col] + year_cols + sales_scope_columns)
    )
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "price_value", "chart": "powertrain_vs_price", "rows": 0, "items": []}

    band_size = int(opts.get("band_size", 1000))
    df["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    df["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    df["Sales"] = _sum_sales_columns(df, sales_scope_columns or year_cols)
    df = df.dropna(subset=["MSRP"])
    df = df[df["MSRP"] > 0]
    df["PriceBand"] = _make_price_bands(df["MSRP"], band_size)

    grouped = df.groupby(["PriceBand", "Powertrain"], dropna=False)["Sales"].sum().reset_index()
    grouped = grouped.sort_values("PriceBand")
    items = grouped.to_dict(orient="records")
    return {"group": "price_value", "chart": "powertrain_vs_price", "rows": len(items), "items": items}


# ── Chart: Estimated TCO vs MSRP ──────────────────────────────
def _chart_estimated_tco(
    filters: dict[str, list[str]], top_n: int, opts: dict,
) -> dict:
    sales_columns = _resolve_sales_columns_from_options(repo.list_columns(), opts)
    vf = _build_vehicle_frame(filters, sales_columns=sales_columns)
    if vf.empty or "MSRP" not in vf.columns or "Powertrain" not in vf.columns:
        return {"group": "cost_analysis", "chart": "estimated_tco", "rows": 0, "items": []}

    vf = vf.dropna(subset=["MSRP"])
    vf = vf[vf["MSRP"] > 0]

    group_cols = [c for c in ["Segment", "Brand", "Model", "Powertrain"] if c in vf.columns]
    if not group_cols:
        return {"group": "cost_analysis", "chart": "estimated_tco", "rows": 0, "items": []}

    agg = vf.groupby(group_cols, dropna=False).agg(
        MSRP=("MSRP", "median"), Sales=("Sales", "sum"),
    ).reset_index()
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))

    years = float(opts.get("years", 5))
    annual_km = float(opts.get("annual_km", 15000))
    depreciation_rate = float(opts.get("depreciation_rate", 0.5))
    maintenance_rate = float(opts.get("maintenance_rate", 0.018))
    tax_insurance_rate = float(opts.get("tax_insurance_rate", 0.02))
    energy_cost_base = float(opts.get("energy_cost_base", 0.1))

    def calc_tco(row):
        msrp = float(row["MSRP"])
        pt = str(row.get("Powertrain", "ICE"))
        ef = POWERTRAIN_ENERGY_FACTOR.get(pt, 1.0)
        depreciation = msrp * depreciation_rate
        energy = annual_km * years * energy_cost_base * ef
        maintenance = msrp * maintenance_rate * years
        tax_insurance = msrp * tax_insurance_rate * years
        return depreciation + energy + maintenance + tax_insurance

    agg["EstimatedTCO"] = agg.apply(calc_tco, axis=1)
    items = agg.to_dict(orient="records")
    return {"group": "cost_analysis", "chart": "estimated_tco", "rows": len(items), "items": items}


# ── CRUD helpers (unchanged) ───────────────────────────────────


# ── Chart: Model Version Bubble ────────────────────────────────
TRIM_CANDIDATES = ["Trim level", "Trim Level", "trim level"]


def _build_version_frame(
    filters: dict[str, list[str]],
    sales_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load version-level frame: Version, Trim, Powertrain, Length, MSRP, Sales."""
    columns = repo.list_columns()
    model_col = _resolve_existing_column(MODEL_CANDIDATES, columns)
    version_col = _resolve_existing_column(VERSION_CANDIDATES, columns)
    make_col = _resolve_existing_column(MAKE_CANDIDATES, columns)
    powertrain_col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
    length_col = _resolve_existing_column(LENGTH_CANDIDATES, columns)
    msrp_col = _resolve_existing_column(MSRP_CANDIDATES, columns)
    trim_col = _resolve_existing_column(TRIM_CANDIDATES, columns)
    year_cols = _year_columns(columns)

    explicit_sales_scope = sales_columns is not None
    requested_sales_columns = [column for column in (sales_columns or []) if column]
    if explicit_sales_scope and not requested_sales_columns:
        return pd.DataFrame()
    needed = (
        [
            c
            for c in [
                model_col,
                version_col,
                make_col,
                powertrain_col,
                length_col,
                msrp_col,
                trim_col,
            ]
            if c
        ]
        + year_cols
        + requested_sales_columns
    )
    needed = list(dict.fromkeys(needed))
    if not needed:
        return pd.DataFrame()

    df = repo.load_slice(columns=needed, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return df

    out = pd.DataFrame(index=df.index)
    if make_col and make_col in df.columns:
        out["Brand"] = df[make_col].astype(str).str.strip()
    if model_col and model_col in df.columns:
        out["Model"] = df[model_col].astype(str).str.strip()
    if version_col and version_col in df.columns:
        out["Version"] = df[version_col].astype(str).str.strip()
    if trim_col and trim_col in df.columns:
        out["Trim"] = df[trim_col].astype(str).str.strip()
    if powertrain_col and powertrain_col in df.columns:
        out["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    if length_col and length_col in df.columns:
        out["Length"] = pd.to_numeric(df[length_col], errors="coerce")
    if msrp_col and msrp_col in df.columns:
        out["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    out["Sales"] = _sum_sales_columns(df, requested_sales_columns or year_cols)
    return out


def query_model_versions(
    filters: dict[str, list[str]],
    model_name: str,
    top_n: int,
    sales_columns: list[str] | None = None,
    time_range: dict[str, str] | None = None,
) -> dict:
    """Return version-level scatter for a given Model."""
    resolved_sales_columns = sales_columns
    normalized_time_range = _extract_time_range(time_range)
    if resolved_sales_columns is None and normalized_time_range is not None:
        resolved_sales_columns = _sales_columns_for_time_range(
            repo.list_columns(),
            normalized_time_range[0],
            normalized_time_range[1],
        )
    vf = _build_version_frame(filters, sales_columns=resolved_sales_columns)
    if vf.empty or "Model" not in vf.columns or "Version" not in vf.columns:
        return {"rows": 0, "items": []}

    vf = vf[vf["Model"].str.lower() == model_name.strip().lower()]
    vf = vf.dropna(subset=["MSRP"])
    vf = vf[vf["MSRP"] > 0]

    group_cols = ["Version"]
    for c in ["Powertrain", "Trim"]:
        if c in vf.columns:
            group_cols.append(c)

    if vf.empty:
        return {"rows": 0, "items": []}

    records: list[dict] = []
    for keys, g in vf.groupby(group_cols, dropna=False):
        sales = g["Sales"].fillna(0)
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        row["MSRP"] = _weighted_median(g["MSRP"], sales)
        if "Length" in g.columns:
            length_valid = g["Length"].dropna()
            row["Length"] = float(length_valid.median()) if len(length_valid) else 0.0
        else:
            row["Length"] = 0.0
        row["Sales"] = float(sales.sum())
        records.append(row)

    agg = pd.DataFrame(records)
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))
    items = agg.to_dict(orient="records")
    return {"rows": len(items), "items": items}


# ── Chart: OJ Positioning Map (KMeans + manual input) ──────────
def query_positioning_map(
    filters: dict[str, list[str]],
    target_length: float | None,
    target_msrp: float | None,
    length_range: float,
    manual_competitors: list[str] | None,
    top_n: int,
    n_clusters: int,
    sales_columns: list[str] | None = None,
    time_range: dict[str, str] | None = None,
) -> dict:
    """
    Return positioning scatter with optional KMeans clustering.
    - If target_length/target_msrp provided, filter to nearby vehicles and cluster.
    - manual_competitors: Brand names to force-include.
    - Returns items with cluster_id, and a target_point if target given.
    """
    resolved_sales_columns = sales_columns
    normalized_time_range = _extract_time_range(time_range)
    if resolved_sales_columns is None and normalized_time_range is not None:
        resolved_sales_columns = _sales_columns_for_time_range(
            repo.list_columns(),
            normalized_time_range[0],
            normalized_time_range[1],
        )
    vf = _build_vehicle_frame(filters, sales_columns=resolved_sales_columns)
    if vf.empty or "Length" not in vf.columns or "MSRP" not in vf.columns:
        return {"rows": 0, "items": [], "target": None, "cluster_top3": [], "peerCorridor": None}

    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[(vf["Length"] > 0) & (vf["MSRP"] > 0)]

    group_cols = [c for c in ["Brand", "Model", "Segment", "Powertrain"] if c in vf.columns]
    if not group_cols:
        return {"rows": 0, "items": [], "target": None, "cluster_top3": [], "peerCorridor": None}

    records: list[dict] = []
    for keys, g in vf.groupby(group_cols, dropna=False):
        sales = g["Sales"].fillna(0)
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        row["Length"] = _weighted_median(g["Length"], sales)
        row["MSRP"] = _weighted_median(g["MSRP"], sales)
        row["Sales"] = float(sales.sum())
        records.append(row)
    agg = pd.DataFrame(records)

    # If target given, narrow window to ±length_range mm
    if target_length is not None and target_length > 0:
        half = max(100, float(length_range) / 2)
        agg = agg[(agg["Length"] >= target_length - half) & (agg["Length"] <= target_length + half)]

    agg = agg.sort_values("Sales", ascending=False)

    # Force-include manual competitors
    if manual_competitors:
        lower_set = {b.strip().lower() for b in manual_competitors}
        if "Brand" in agg.columns:
            forced = agg[agg["Brand"].str.lower().isin(lower_set)]
            rest = agg[~agg["Brand"].str.lower().isin(lower_set)].head(max(1, int(top_n)))
            agg = pd.concat([forced, rest]).drop_duplicates(subset=["Model"] if "Model" in agg.columns else group_cols)
        else:
            agg = agg.head(max(1, int(top_n)))
    else:
        agg = agg.head(max(1, int(top_n)))

    peer_corridor = _build_peer_corridor(
        agg,
        target_length=target_length,
        target_msrp=target_msrp,
    )

    # KMeans clustering
    cluster_top3: list[str] = []
    if len(agg) >= n_clusters and "Length" in agg.columns and "MSRP" in agg.columns:
        from sklearn.preprocessing import StandardScaler
        from sklearn.cluster import KMeans as _KMeans

        features = agg[["Length", "MSRP", "Sales"]].fillna(0).values
        scaler = StandardScaler()
        scaled = scaler.fit_transform(features)
        km = _KMeans(n_clusters=min(n_clusters, len(agg)), n_init=10, random_state=42)
        agg = agg.copy()
        agg["cluster"] = km.fit_predict(scaled)

        # Top-3 clusters by total sales
        cluster_sales = agg.groupby("cluster")["Sales"].sum().sort_values(ascending=False)
        top_cluster_ids = list(cluster_sales.head(3).index)
        cluster_top3 = []
        for cid in top_cluster_ids:
            rows_in_c = agg[agg["cluster"] == cid]
            best = rows_in_c.sort_values("Sales", ascending=False).iloc[0]
            label = str(best.get("Brand", "")) + " " + str(best.get("Model", ""))
            cluster_top3.append(label.strip())
    else:
        agg = agg.copy()
        agg["cluster"] = 0

    items = agg.to_dict(orient="records")
    target_point = None
    if target_length is not None and target_msrp is not None:
        target_point = {"Length": float(target_length), "MSRP": float(target_msrp)}

    return {
        "rows": len(items),
        "items": items,
        "target": target_point,
        "cluster_top3": cluster_top3,
        "peerCorridor": peer_corridor,
    }


# ── RV Finance Calculator ───────────────────────────────

COUNTRY_FINANCE_PRESETS: dict[str, dict] = {
    "瑞典":   {"down_pct": 20, "rv_pct": 45, "apr_pct": 3.5, "term": 36},
    "挪威":   {"down_pct": 15, "rv_pct": 50, "apr_pct": 3.0, "term": 36},
    "德国":   {"down_pct": 20, "rv_pct": 48, "apr_pct": 3.9, "term": 36},
    "法国":   {"down_pct": 20, "rv_pct": 40, "apr_pct": 4.0, "term": 48},
    "英国":   {"down_pct": 10, "rv_pct": 42, "apr_pct": 5.9, "term": 48},
    "荷兰":   {"down_pct": 15, "rv_pct": 45, "apr_pct": 3.5, "term": 36},
}

CURRENCY_RATES: dict[str, float] = {
    "EUR": 1.0, "SEK": 11.5, "NOK": 11.3, "DKK": 7.46, "GBP": 0.86, "USD": 1.09,
}


def _pmt(rate_monthly: float, n_periods: int, present_value: float, future_value: float) -> float:
    """Standard annuity payment formula (Excel PMT equivalent)."""
    if rate_monthly == 0:
        return -(present_value + future_value) / max(n_periods, 1)
    q = (1 + rate_monthly) ** n_periods
    return -(present_value * q + future_value) * rate_monthly / (q - 1)


def calculate_rv_finance(vehicles: list[dict]) -> list[dict]:
    """Pure finance calculation for a list of vehicle parameter dicts."""
    results = []
    for v in vehicles:
        msrp = float(v.get("msrp", 0))
        down_pct = float(v.get("down_pct", 20)) / 100
        rv_pct = float(v.get("rv_pct", 45)) / 100
        apr_pct = float(v.get("apr_pct", 3.5)) / 100
        term = int(v.get("term", 36))
        name = str(v.get("vehicle", "Vehicle"))

        down = msrp * down_pct
        principal = msrp - down
        balloon = msrp * rv_pct
        pv_rv = balloon / ((1 + apr_pct / 12) ** term) if apr_pct > 0 else balloon
        net_financed = principal - pv_rv
        monthly = abs(_pmt(apr_pct / 12, term, net_financed, 0)) if term > 0 else 0
        total_payments = monthly * term

        results.append({
            "vehicle": name, "msrp": msrp,
            "down_pct": round(down_pct * 100, 1), "rv_pct": round(rv_pct * 100, 1),
            "apr_pct": round(apr_pct * 100, 2), "term": term,
            "down": round(down, 2), "principal": round(principal, 2),
            "balloon": round(balloon, 2), "pv_rv": round(pv_rv, 2),
            "net_financed": round(net_financed, 2),
            "monthly": round(monthly, 2), "total_payments": round(total_payments, 2),
        })
    return results


def query_rv_finance(
    vehicles: list[dict],
    currency: str = "EUR",
    fx_rate: float | None = None,
    sensitivity_vehicle_idx: int = 0,
) -> dict:
    """Full RV finance endpoint: calculate, waterfall, sensitivity."""
    rate = float(fx_rate) if fx_rate is not None and float(fx_rate) > 0 else CURRENCY_RATES.get(currency, 1.0)
    converted = []
    for v in vehicles:
        cv = {**v, "msrp": float(v.get("msrp", 0)) * rate}
        converted.append(cv)

    results = calculate_rv_finance(converted)

    # Waterfall for first vehicle (or sensitivity_vehicle_idx)
    idx = min(sensitivity_vehicle_idx, len(results) - 1) if results else 0
    waterfall = []
    if results:
        r = results[idx]
        waterfall = [
            {"label": "MSRP", "value": r["msrp"], "type": "total"},
            {"label": "Down Payment", "value": -r["down"], "type": "relative"},
            {"label": "Principal", "value": r["principal"], "type": "total"},
            {"label": "PV(RV)", "value": -r["pv_rv"], "type": "relative"},
            {"label": "Net Financed", "value": r["net_financed"], "type": "total"},
        ]

    # Sensitivity analysis for selected vehicle
    sensitivity = []
    if results:
        base = converted[idx]
        base_monthly = results[idx]["monthly"]
        for param, label, low_delta, high_delta in [
            ("apr_pct", "APR %", -1.0, 1.0),
            ("rv_pct", "RV %", -10, 10),
            ("down_pct", "Down %", -5, 5),
            ("term", "Term (months)", -12, 12),
        ]:
            base_val = float(base.get(param, 0))
            for scenario, delta in [("low", low_delta), ("high", high_delta)]:
                varied = {**base, param: base_val + delta}
                calc = calculate_rv_finance([varied])[0]
                sensitivity.append({
                    "param": label, "scenario": scenario,
                    "param_value": round(base_val + delta, 2),
                    "monthly": calc["monthly"],
                    "delta": round(calc["monthly"] - base_monthly, 2),
                })

    # Contour: APR × RV grid (9×9)
    contour = {"apr_values": [], "rv_values": [], "z": []}
    if results:
        base = converted[idx]
        apr_base = float(base.get("apr_pct", 3.5))
        rv_base = float(base.get("rv_pct", 45))
        apr_vals = [round(apr_base + d, 1) for d in [-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2]]
        rv_vals = [round(rv_base + d, 0) for d in [-20, -15, -10, -5, 0, 5, 10, 15, 20]]
        contour["apr_values"] = apr_vals
        contour["rv_values"] = rv_vals
        z_matrix = []
        for rv in rv_vals:
            row = []
            for apr in apr_vals:
                varied = {**base, "apr_pct": apr, "rv_pct": rv}
                calc = calculate_rv_finance([varied])[0]
                row.append(round(calc["monthly"], 2))
            z_matrix.append(row)
        contour["z"] = z_matrix

    return {
        "results": results,
        "waterfall": waterfall,
        "sensitivity": sensitivity,
        "contour": contour,
        "currency": currency,
        "rate": rate,
        "presets": COUNTRY_FINANCE_PRESETS,
    }


def list_items() -> list[dict]:
    return repo.list_crud_items()


def list_items_query(
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
    query: str,
) -> dict:
    return repo.list_crud_items_query(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order,
        query=query,
    )


def get_item(item_id: str) -> dict | None:
    items = repo.list_crud_items()
    return next((row for row in items if row.get("id") == item_id), None)


def create_item(data: dict) -> dict:
    item = {"id": str(uuid4()), **data}
    repo.upsert_crud_item(item)
    return item


def update_item(item_id: str, data: dict) -> dict | None:
    items = repo.list_crud_items()
    existing = next((row for row in items if row.get("id") == item_id), None)
    if not existing:
        return None
    patched = {**existing, **{k: v for k, v in data.items() if v is not None}}
    repo.upsert_crud_item(patched)
    return patched


def delete_item(item_id: str) -> bool:
    items = repo.list_crud_items()
    filtered = [row for row in items if row.get("id") != item_id]
    if len(filtered) == len(items):
        return False
    from app.infra.parquet_repository import _write_crud_items
    _write_crud_items(filtered)
    return True


remove_item = delete_item
