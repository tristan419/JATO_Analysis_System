from __future__ import annotations

import logging
import threading
import time
from typing import Any

import numpy as np
import pandas as pd

from app.infra import parquet_repository as repo
from app.services.market_scan_service import (
    _get_columns,
    _month_column_to_period,
    _resolve_existing_column,
)

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 600
_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()

DRIVE_NORMALIZE_RULES: dict[str, list[str]] = {
    "4WD": ["awd", "4wd", "4x4", "all wheel", "quattro", "xdrive"],
    "2WD": ["fwd", "rwd", "2wd", "front wheel", "rear wheel", "sdrive"],
}
REGISTRATION_NORMALIZE_RULES: dict[str, list[str]] = {
    "Business": ["business", "fleet", "company", "corporate", "lease", "rental"],
    "Private": ["private", "retail", "personal", "consumer"],
}


def _cache_key(prefix: str, **kwargs) -> str:
    raw = f"{prefix}:" + ",".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    return raw


def _cached_or_compute(key: str, compute_fn, ttl: int = _CACHE_TTL_SECONDS) -> dict[str, Any]:
    now = time.time()
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (now - entry[0]) < ttl:
            return entry[1]
    result = compute_fn()
    with _cache_lock:
        _cache[key] = (now, result)
    return result


def _normalize_drive(raw: str) -> str:
    if not raw:
        return "OTHER"
    text = str(raw).strip().lower()
    for label, patterns in DRIVE_NORMALIZE_RULES.items():
        for pat in patterns:
            if pat in text:
                return label
    return "OTHER"


def _normalize_registration(raw: str) -> str:
    if not raw:
        return "Other"
    text = str(raw).strip().lower()
    for label, patterns in REGISTRATION_NORMALIZE_RULES.items():
        for pat in patterns:
            if pat in text:
                return label
    return "Other"


def _apply_scope_filters(fact: pd.DataFrame, scope_filters: list[dict[str, str]] | None) -> pd.DataFrame:
    """Apply page scope filters, treating repeated dimensions as OR selections."""
    if not scope_filters:
        return fact
    values_by_dim: dict[str, set[str]] = {}
    for scope_filter in scope_filters:
        dim = scope_filter.get("dim")
        value = scope_filter.get("value")
        if dim and value and dim in fact.columns:
            values_by_dim.setdefault(dim, set()).add(value)
    filtered = fact
    for dim, values in values_by_dim.items():
        filtered = filtered[filtered[dim].isin(values)]
    return filtered


PROFILE_FILTER_DIMS = ["segment", "body_type", "powertrain", "registration_type", "drive_type", "origin", "make"]
PRODUCT_SPEC_CANDIDATES: dict[str, list[str]] = {
    "length_mm": ["length (mm)", "车长(mm)", "车长", "length"],
    "msrp": ["MSRP规整", "MSRP including delivery charge", "MSRP", "MSRP区间"],
    "ev_range": ["Battery range", "Electric range", "Electric range (km)", "WLTP electric range", "Range (km)"],
    "fuel_consumption": ["Fuel consumption combined", "WLTP Consumption combined", "Combined fuel consumption", "Fuel economy combined", "Consumption combined"],
    "co2_emission": ["WLTP Emission combined", "CO2 level - (g/km) combined", "CO2 combined"],
    "battery_kwh": ["Useable battery kilowatt hour (kWh)", "Battery kwh", "Battery capacity"],
}
PRODUCT_SPEC_LABELS: dict[str, str] = {
    "length_mm": "Length",
    "msrp": "MSRP",
    "ev_range": "EV range",
    "fuel_consumption": "Fuel consumption",
    "co2_emission": "CO2",
    "battery_kwh": "Battery",
}
CATEGORICAL_MATCH_WEIGHTS: dict[str, float] = {
    "powertrain": 28.0,
    "segment": 16.0,
    "body_type": 10.0,
    "registration_type": 6.0,
    "drive_type": 6.0,
    "origin": 4.0,
    "make": 2.0,
}
NUMERIC_MATCH_WEIGHTS: dict[str, float] = {
    "length_mm": 14.0,
    "msrp": 12.0,
    "ev_range": 10.0,
    "fuel_consumption": 6.0,
    "co2_emission": 4.0,
    "battery_kwh": 4.0,
}
NUMERIC_MATCH_TOLERANCES: dict[str, float] = {
    "length_mm": 450.0,
    "msrp": 5000.0,
    "ev_range": 120.0,
    "fuel_consumption": 1.0,
    "co2_emission": 40.0,
    "battery_kwh": 20.0,
}


def _scope_filter_values(scope_filters: list[dict[str, str]] | None) -> dict[str, set[str]]:
    values_by_dim: dict[str, set[str]] = {}
    for scope_filter in scope_filters or []:
        dim = scope_filter.get("dim")
        value = scope_filter.get("value")
        if dim and value:
            values_by_dim.setdefault(dim, set()).add(value)
    return values_by_dim


def _resolve_product_spec_columns(column_names: list[str]) -> dict[str, str]:
    return {
        field: column
        for field, candidates in PRODUCT_SPEC_CANDIDATES.items()
        if (column := _resolve_existing_column(candidates, column_names))
    }


def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    extracted = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    )
    return pd.to_numeric(extracted, errors="coerce")


def _numeric_value(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return numeric


def _normalize_profile_specs(profile_specs: dict[str, Any] | None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for field in NUMERIC_MATCH_WEIGHTS:
        value = _numeric_value((profile_specs or {}).get(field))
        if value is None:
            continue
        if value > 0 or field == "co2_emission":
            normalized[field] = value
    return normalized


def _numeric_match_score(field: str, target_value: float, candidate_value: float) -> float:
    tolerance = NUMERIC_MATCH_TOLERANCES[field]
    if field in {"msrp", "fuel_consumption"}:
        tolerance = max(tolerance, abs(target_value) * 0.25)
    diff = abs(candidate_value - target_value)
    return max(0.0, 1.0 - diff / (tolerance * 2.0))


def _format_spec_value(field: str, value: float) -> str:
    if field == "length_mm":
        return f"{value:.0f} mm"
    if field == "msrp":
        return f"{value:,.0f}"
    if field == "ev_range":
        return f"{value:.0f} km"
    if field == "fuel_consumption":
        return f"{value:.1f}"
    if field == "co2_emission":
        return f"{value:.0f} g/km"
    if field == "battery_kwh":
        return f"{value:.1f} kWh"
    return f"{value:.1f}"


def list_profile_filter_options(country: str | None = None) -> dict[str, Any]:
    """Return normalized profile dimensions used by Advanced Analysis filters."""
    key = _cache_key("profile_options", country=country)

    def _compute():
        fact = build_fact_sales_monthly(country=country)
        options: dict[str, list[str]] = {}
        for dim in [*PROFILE_FILTER_DIMS, "model"]:
            if dim not in fact.columns:
                options[dim] = []
                continue
            values = (
                fact[dim]
                .dropna()
                .astype(str)
                .map(lambda value: value.strip())
            )
            options[dim] = sorted(value for value in values.unique().tolist() if value)
        return {"country": country, "options": options}

    return _cached_or_compute(key, _compute)


def build_fact_sales_monthly(
    country: str | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
) -> pd.DataFrame:
    """Wide-to-long transformation producing a normalized fact table."""
    cols = _get_columns()
    column_names = repo.list_columns()
    spec_columns = _resolve_product_spec_columns(column_names)

    # Build the set of columns to load
    id_cols = [cols.country_value, cols.segment, cols.make, cols.model, cols.powertrain]
    for opt in [cols.drive_type, cols.registration_type, cols.body_type, cols.origin, *spec_columns.values()]:
        if opt and opt in column_names and opt not in id_cols:
            id_cols.append(opt)

    month_cols = [c for c in cols.month_columns if c in column_names]
    load_cols = id_cols + month_cols

    # Load via PyArrow, optionally filtering by country
    dataset = repo._open_dataset()
    if country and cols.country_value in column_names:
        filter_expr = repo._build_filter_expression({cols.country_value: [country]})
        table = dataset.to_table(columns=load_cols, filter=filter_expr)
    else:
        table = dataset.to_table(columns=load_cols)

    df = table.to_pandas()

    if fuel_types:
        df = df[df[cols.powertrain].isin(fuel_types)]
    if segments:
        df = df[df[cols.segment].isin(segments)]

    id_vars = [cols.country_value, cols.segment, cols.make, cols.model, cols.powertrain]
    for opt in [cols.drive_type, cols.registration_type, cols.body_type, cols.origin, *spec_columns.values()]:
        if opt and opt in df.columns and opt not in id_vars:
            id_vars.append(opt)

    available = [c for c in id_vars if c in df.columns]
    available_month_cols = [c for c in month_cols if c in df.columns]

    long = df.melt(
        id_vars=available,
        value_vars=available_month_cols,
        var_name="month_col",
        value_name="sales",
    )

    rename_map = {
        cols.country_value: "country",
        cols.segment: "segment",
        cols.make: "make",
        cols.model: "model",
        cols.powertrain: "powertrain",
    }
    if cols.drive_type and cols.drive_type in available:
        rename_map[cols.drive_type] = "drive_type"
    if cols.registration_type and cols.registration_type in available:
        rename_map[cols.registration_type] = "registration_type"
    if cols.body_type and cols.body_type in available:
        rename_map[cols.body_type] = "body_type"
    if cols.origin and cols.origin in available:
        rename_map[cols.origin] = "origin"
    for field, raw_col in spec_columns.items():
        if raw_col in available:
            rename_map[raw_col] = field

    long = long.rename(columns=rename_map)
    long["period"] = long["month_col"].apply(_month_column_to_period)
    long["sales"] = pd.to_numeric(long["sales"], errors="coerce").fillna(0.0)
    for field in PRODUCT_SPEC_CANDIDATES:
        if field in long.columns:
            long[field] = _coerce_numeric_series(long[field])

    if "drive_type" in long.columns:
        long["drive_type"] = long["drive_type"].apply(_normalize_drive)
    else:
        long["drive_type"] = "OTHER"
    if "registration_type" in long.columns:
        long["registration_type"] = long["registration_type"].apply(_normalize_registration)
    else:
        long["registration_type"] = "Other"

    return long


def compute_kpi_table(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    group_by: list[str] | None = None,
    top_n: int = 50,
) -> dict[str, Any]:
    """Compute YoY, MoM, YTD, rolling-12, and share% KPIs."""
    if group_by is None:
        group_by = ["segment", "model"]

    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        group_by=",".join(group_by), top_n=top_n)
    key = _cache_key("kpi", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)

        grp_cols = [c for c in group_by if c in fact.columns]
        base = fact.groupby(grp_cols + ["period"], as_index=False)["sales"].sum()
        base = base.sort_values(grp_cols + ["period"])

        base["mom"] = base.groupby(grp_cols)["sales"].pct_change(1)
        base["yoy"] = base.groupby(grp_cols)["sales"].pct_change(12)

        base["trailing12_sum"] = base.groupby(grp_cols)["sales"].transform(
            lambda s: s.rolling(12, min_periods=1).sum()
        )
        base["rolling12_avg"] = base.groupby(grp_cols)["sales"].transform(
            lambda s: s.rolling(12, min_periods=1).mean()
        )

        # Market share within the group's market (e.g., within same segment+period)
        share_grp = [c for c in grp_cols if c != "model"] if "model" in grp_cols else grp_cols
        if share_grp:
            market = base.groupby(share_grp + ["period"], as_index=False)["sales"].sum()
            market = market.rename(columns={"sales": "market_sales"})
            base = base.merge(market, on=share_grp + ["period"], how="left")
            base["share"] = np.where(base["market_sales"] > 0, base["sales"] / base["market_sales"], 0.0)
        else:
            total = base.groupby("period", as_index=False)["sales"].sum()
            total = total.rename(columns={"sales": "market_sales"})
            base = base.merge(total, on="period", how="left")
            base["share"] = np.where(base["market_sales"] > 0, base["sales"] / base["market_sales"], 0.0)

        base = base.replace([np.inf, -np.inf], 0.0).fillna(0.0)

        if target_period:
            base = base[base["period"] == target_period]
        elif time_range:
            base = base[(base["period"] >= time_range["start"]) & (base["period"] <= time_range["end"])]

        # Top N by sales
        latest = base["period"].max()
        latest_rows = base[base["period"] == latest]
        top_agg = latest_rows.groupby(grp_cols, as_index=False)["sales"].sum()
        top_agg = top_agg.nlargest(top_n, "sales")
        if grp_cols:
            # Build a set of tuples for fast membership check
            top_set = set()
            for _, r in top_agg.iterrows():
                key = tuple(r[c] for c in grp_cols)
                top_set.add(key if len(grp_cols) > 1 else key[0])
            mask = base[grp_cols].apply(
                lambda row: (tuple(row[c] for c in grp_cols) if len(grp_cols) > 1 else row[grp_cols[0]]) in top_set,
                axis=1,
            )
            base = base[mask]

        rows = []
        for _, row in base.iterrows():
            d: dict[str, Any] = {}
            for c in grp_cols:
                d[c] = row.get(c)
            d["period"] = row["period"]
            d["sales"] = row["sales"]
            d["mom"] = row.get("mom")
            d["yoy"] = row.get("yoy")
            d["trailing12_sum"] = row.get("trailing12_sum")
            d["rolling12_avg"] = row.get("rolling12_avg")
            d["share"] = row.get("share")
            rows.append(d)

        return {"group_by": grp_cols, "rows": rows, "total_rows": len(rows)}

    return _cached_or_compute(key, _compute)


def compute_shift_share_decomposition(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    base_period: str | None = None,
    cell_dims: list[str] | None = None,
) -> dict[str, Any]:
    """
    Single-layer shift-share decomposition:
      dV = s_0 * dM  +  M_0 * ds  +  ds * dM

    Returns winner/loser lists with market_growth_effect, share_shift_effect, interaction_effect.
    """
    if cell_dims is None:
        cell_dims = ["segment"]

    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        base_period=base_period,
                        cell_dims=",".join(cell_dims))
    key = _cache_key("shift_share", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)

        # Model identity columns: model + contextual dims not in cell_key
        candidate_model_cols = ["model"]
        cell_key = [c for c in cell_dims if c in fact.columns]
        if not cell_key:
            cell_key = ["segment"] if "segment" in fact.columns else []
        # segment can be in model_key OR cell_key, not both
        model_key = [c for c in candidate_model_cols if c in fact.columns]

        # Aggregate to model × cell × period
        agg_key = list(dict.fromkeys(model_key + cell_key + ["period"]))  # dedup
        x = fact.groupby(agg_key, as_index=False)["sales"].sum()

        # Cell-level market total
        cell_total = x.groupby(cell_key + ["period"], as_index=False)["sales"].sum()
        cell_total = cell_total.rename(columns={"sales": "M"})
        x = x.merge(cell_total, on=cell_key + ["period"], how="left")
        x["s"] = np.where(x["M"] > 0, x["sales"] / x["M"], 0.0)

        # Determine base and target periods
        periods = sorted(x["period"].unique())
        if not periods:
            return {"error": "No data for selected filters"}

        tgt = target_period or (time_range["end"] if time_range else periods[-1])
        base = base_period or _shift_period(tgt, -12, periods) or periods[0]

        base_data = x[x["period"] == base].copy()
        tgt_data = x[x["period"] == tgt].copy()

        merge_on = list(dict.fromkeys(model_key + cell_key))
        merged = tgt_data.merge(
            base_data[merge_on + ["sales", "M", "s"]],
            on=merge_on, how="outer", suffixes=("", "_0")
        )
        for col in ["sales_0", "M_0", "s_0"]:
            merged[col] = merged[col].fillna(0.0)
        merged["sales"] = merged["sales"].fillna(0.0)
        merged["M"] = merged["M"].fillna(0.0)
        merged["s"] = merged["s"].fillna(0.0)

        merged["dV"] = merged["sales"] - merged["sales_0"]
        merged["dM"] = merged["M"] - merged["M_0"]
        merged["ds"] = merged["s"] - merged["s_0"]

        merged["market_growth_effect"] = merged["s_0"] * merged["dM"]
        merged["share_shift_effect"] = merged["M_0"] * merged["ds"]
        merged["interaction_effect"] = merged["ds"] * merged["dM"]

        merged = merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)

        winners = merged[merged["share_shift_effect"] > 0].nlargest(20, "share_shift_effect")
        losers = merged[merged["share_shift_effect"] < 0].nsmallest(20, "share_shift_effect")

        def _row(r, extra: dict | None = None):
            d: dict[str, Any] = {}
            for c in model_key + cell_key:
                d[c] = r.get(c)
            d["sales"] = r["sales"]
            d["sales_0"] = r.get("sales_0")
            d["dV"] = r.get("dV")
            d["market_growth_effect"] = r.get("market_growth_effect")
            d["share_shift_effect"] = r.get("share_shift_effect")
            d["interaction_effect"] = r.get("interaction_effect")
            if extra:
                d.update(extra)
            return d

        return {
            "base_period": base,
            "target_period": tgt,
            "cell_dims": cell_key,
            "total_market_delta": float(merged["dV"].sum()) if len(merged) > 0 else 0.0,
            "winners": [_row(r) for _, r in winners.iterrows()],
            "losers": [_row(r) for _, r in losers.iterrows()],
        }

    return _cached_or_compute(key, _compute)


def _shift_period(period: str, offset: int, available: list[str]) -> str | None:
    """Shift a YYYY-MM period by offset months within available periods."""
    try:
        year, month = int(period[:4]), int(period[5:7])
    except (ValueError, IndexError):
        return None
    total_months = year * 12 + (month - 1) + offset
    new_year, new_month = divmod(total_months, 12)
    new_period = f"{new_year:04d}-{new_month + 1:02d}"
    return new_period if new_period in available else None


def _period_offset(period: str, offset: int) -> str | None:
    """Return YYYY-MM shifted by offset months without checking availability."""
    try:
        year, month = int(period[:4]), int(period[5:7])
    except (ValueError, IndexError):
        return None
    total_months = year * 12 + (month - 1) + offset
    if total_months < 0:
        return None
    new_year, new_month = divmod(total_months, 12)
    return f"{new_year:04d}-{new_month + 1:02d}"


def _period_window(available: list[str], end_period: str, sales_mode: str) -> list[str]:
    if sales_mode == "ytd":
        start_period = f"{end_period[:4]}-01"
    elif sales_mode == "rolling12":
        start_period = _period_offset(end_period, -11) or end_period
    else:
        start_period = end_period
    return [period for period in available if start_period <= period <= end_period]


def _aggregate_fact_for_sales_mode(
    fact: pd.DataFrame,
    available_periods: list[str],
    target_period: str,
    base_period: str,
    sales_mode: str,
) -> pd.DataFrame:
    """Aggregate monthly fact rows into target/base windows for transfer analysis."""
    if sales_mode == "month":
        return fact

    target_window = _period_window(available_periods, target_period, sales_mode)
    base_window = _period_window(available_periods, base_period, sales_mode)
    if not target_window or not base_window:
        return fact

    group_cols = [col for col in fact.columns if col not in {"sales", "period", "month_col"}]

    def _window_frame(window: list[str], label: str) -> pd.DataFrame:
        frame = fact[fact["period"].isin(window)]
        if group_cols:
            frame = frame.groupby(group_cols, as_index=False, dropna=False)["sales"].sum()
        else:
            frame = pd.DataFrame({"sales": [float(frame["sales"].sum())]})
        frame["period"] = label
        return frame

    return pd.concat([
        _window_frame(base_window, base_period),
        _window_frame(target_window, target_period),
    ], ignore_index=True)


def compute_cell_attribution(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    cell_dims: list[str] | None = None,
    top_n_cells: int = 20,
) -> dict[str, Any]:
    """Compute share_shift_effect aggregated at the cell level for heatmap display."""
    if cell_dims is None:
        cell_dims = ["segment", "registration_type", "drive_type"]

    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        cell_dims=",".join(cell_dims), top_n_cells=top_n_cells)
    key = _cache_key("cell_attr", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)
        if len(fact) == 0:
            return {"error": "No data for selected filters"}
        cell_key = [c for c in cell_dims if c in fact.columns]
        if not cell_key:
            cell_key = ["segment"] if "segment" in fact.columns else []

        # Aggregate all sales by cell × period
        cell_sales = fact.groupby(cell_key + ["period"], as_index=False)["sales"].sum()
        cell_sales = cell_sales.sort_values(cell_key + ["period"])

        periods = sorted(cell_sales["period"].unique())
        if not periods:
            return {"error": "No data for selected filters"}
        tgt = target_period or periods[-1]
        base = _shift_period(tgt, -12, periods) or periods[0]

        tgt_data = cell_sales[cell_sales["period"] == tgt]
        base_data = cell_sales[cell_sales["period"] == base]

        merged = tgt_data.merge(base_data, on=cell_key, how="outer", suffixes=("", "_0"))
        merged["sales"] = merged["sales"].fillna(0.0)
        merged["sales_0"] = merged["sales_0"].fillna(0.0)
        merged["dV"] = merged["sales"] - merged["sales_0"]
        merged["yoy_pct"] = np.where(
            merged["sales_0"] > 0,
            (merged["sales"] - merged["sales_0"]) / merged["sales_0"],
            0.0,
        )
        merged["_abs_dV"] = merged["dV"].abs()
        merged = merged.nlargest(top_n_cells, "_abs_dV").drop(columns=["_abs_dV"])

        cells: list[dict[str, Any]] = []
        for _, row in merged.iterrows():
            d: dict[str, Any] = {}
            for c in cell_key:
                d[c] = row.get(c)
            d["sales"] = row["sales"]
            d["sales_0"] = row.get("sales_0")
            d["dV"] = row.get("dV")
            d["yoy_pct"] = row.get("yoy_pct")
            cells.append(d)

        return {"base_period": base, "target_period": tgt, "cell_dims": cell_key, "cells": cells}

    return _cached_or_compute(key, _compute)


def compute_seasonal_decomposition(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    model_filter: str | None = None,
    segment_filter: str | None = None,
) -> dict[str, Any]:
    """STL decomposition of a monthly sales time series using statsmodels."""
    try:
        from statsmodels.tsa.seasonal import STL  # type: ignore
    except ImportError:
        return {"error": "statsmodels not installed; run: pip install statsmodels"}

    tr_key = f"{time_range['start']}_{time_range['end']}" if time_range else ""
    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        time_range=tr_key,
                        model_filter=model_filter, segment_filter=segment_filter)
    key = _cache_key("seasonal", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)
        if time_range:
            fact = fact[(fact["period"] >= time_range["start"]) & (fact["period"] <= time_range["end"])]
        grp_cols = []
        if segment_filter:
            fact = fact[fact["segment"] == segment_filter]
            grp_cols.append("segment")
        if model_filter:
            fact = fact[fact["model"] == model_filter]
            grp_cols.append("model")

        if grp_cols:
            ts = fact.groupby(grp_cols + ["period"], as_index=False)["sales"].sum()
        else:
            ts = fact.groupby("period", as_index=False)["sales"].sum()

        ts = ts.sort_values("period")
        if len(ts) < 24:
            return {"error": f"Need at least 24 monthly observations, got {len(ts)}"}

        series = ts["sales"].values.astype(float)
        try:
            stl = STL(series, period=12, seasonal=13).fit()
        except Exception as exc:
            return {"error": f"STL decomposition failed: {exc}"}

        return {
            "periods": ts["period"].tolist(),
            "observed": series.tolist(),
            "trend": stl.trend.tolist(),
            "seasonal": stl.seasonal.tolist(),
            "resid": stl.resid.tolist(),
            "model_filter": model_filter,
            "segment_filter": segment_filter,
        }

    return _cached_or_compute(key, _compute)


def compute_probabilistic_transfer_matrix(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    cell_dims: list[str] | None = None,
    top_n_models: int = 20,
) -> dict[str, Any]:
    """
    Build a probability-based transfer matrix (Sankey input) within each cell.
    Winner share gains are allocated proportionally to loser share losses.
    """
    if cell_dims is None:
        cell_dims = ["segment"]

    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        cell_dims=",".join(cell_dims), top_n_models=top_n_models)
    key = _cache_key("transfer", **cache_kwargs)

    def _compute():
        result = compute_shift_share_decomposition(
            country=country, target_period=target_period, time_range=time_range,
            fuel_types=fuel_types, segments=segments, cell_dims=cell_dims,
        )
        if "error" in result:
            return result

        winners = result.get("winners", [])
        losers = result.get("losers", [])

        total_loss = sum(abs(l["share_shift_effect"]) for l in losers)
        if total_loss == 0:
            return {"nodes": [], "links": [], "message": "No share loss to attribute"}

        node_labels: list[str] = []
        node_set: dict[str, int] = {}
        links: list[dict[str, Any]] = []

        def _node_idx(label: str) -> int:
            if label not in node_set:
                node_set[label] = len(node_labels)
                node_labels.append(label)
            return node_set[label]

        for loser in losers:
            _node_idx(f"{loser.get('model', '?')} (L)")
        for winner in winners:
            _node_idx(f"{winner.get('model', '?')} (W)")

        for winner in winners:
            win_effect = winner["share_shift_effect"]
            for loser in losers:
                loss_abs = abs(loser["share_shift_effect"])
                estimated_transfer = win_effect * loss_abs / total_loss
                if estimated_transfer > 0.001:
                    links.append({
                        "source": node_set[f"{loser.get('model', '?')} (L)"],
                        "target": node_set[f"{winner.get('model', '?')} (W)"],
                        "value": round(estimated_transfer, 4),
                    })

        return {
            "nodes": [{"label": lbl} for lbl in node_labels],
            "links": links,
            "base_period": result.get("base_period"),
            "target_period": result.get("target_period"),
            "total_transfer_volume": round(sum(l["value"] for l in links), 4),
        }

    return _cached_or_compute(key, _compute)


def compute_nested_shift_share(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    base_period: str | None = None,
    hierarchy: list[str] | None = None,
) -> dict[str, Any]:
    """
    Hierarchical shift-share decomposition across multiple cell levels.

    hierarchy = ["segment", "registration_type", "drive_type"] produces:
      Level 1: segment-level market growth + share shift
      Level 2: within each segment, registration_type effects
      Level 3: within each reg_type, drive_type effects
      Leaf: model-level decomposition within finest cells
    """
    if hierarchy is None:
        hierarchy = ["segment", "registration_type", "drive_type"]

    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        base_period=base_period,
                        hierarchy=",".join(hierarchy))
    key = _cache_key("nested_ss", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)
        available_hierarchy = [h for h in hierarchy if h in fact.columns]
        if not available_hierarchy:
            available_hierarchy = ["segment"] if "segment" in fact.columns else []
        model_key = [c for c in ["model"] if c in fact.columns]

        periods = sorted(fact["period"].unique())
        if not periods:
            return {"error": "No data for selected filters"}
        tgt = target_period or periods[-1]
        base = base_period or _shift_period(tgt, -12, periods) or periods[0]

        levels: list[dict[str, Any]] = []
        # Build progressively finer cell keys
        for level_idx in range(len(available_hierarchy) + 1):
            cell_dims = available_hierarchy[:level_idx]  # empty for level 0 (total market)
            cell_key = cell_dims if cell_dims else []
            agg_key = list(dict.fromkeys(model_key + cell_key + ["period"]))

            x = fact.groupby(agg_key, as_index=False)["sales"].sum()
            if not cell_key:
                # Level 0: total market
                cell_total = x.groupby("period", as_index=False)["sales"].sum()
            else:
                cell_total = x.groupby(cell_key + ["period"], as_index=False)["sales"].sum()
            cell_total = cell_total.rename(columns={"sales": "M"})

            if not cell_key:
                x["M"] = x.merge(cell_total, on="period", how="left")["M"]
            else:
                x = x.merge(cell_total, on=cell_key + ["period"], how="left")
            x["s"] = np.where(x["M"] > 0, x["sales"] / x["M"], 0.0)

            base_data = x[x["period"] == base].copy()
            tgt_data = x[x["period"] == tgt].copy()

            if cell_key:
                merge_on = list(dict.fromkeys(model_key + cell_key))
            else:
                merge_on = model_key if model_key else []

            if merge_on:
                merged = tgt_data.merge(
                    base_data[merge_on + ["sales", "M", "s"]],
                    on=merge_on, how="outer", suffixes=("", "_0"),
                )
            else:
                merged = tgt_data.copy()
                for col in ["sales", "M", "s"]:
                    if col in base_data:
                        merged[f"{col}_0"] = base_data[col].values[0] if len(base_data) > 0 else 0.0

            for col in ["sales_0", "M_0", "s_0"]:
                if col in merged.columns:
                    merged[col] = merged[col].fillna(0.0)
            merged["sales"] = merged["sales"].fillna(0.0)
            merged["M"] = merged["M"].fillna(0.0)
            merged["s"] = merged["s"].fillna(0.0)

            merged["dV"] = merged["sales"] - merged["sales_0"]
            merged["dM"] = merged["M"] - merged["M_0"]
            merged["ds"] = merged["s"] - merged["s_0"]
            merged["market_growth_effect"] = merged["s_0"] * merged["dM"]
            merged["share_shift_effect"] = merged["M_0"] * merged["ds"]
            merged["interaction_effect"] = merged["ds"] * merged["dM"]

            merged = merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)

            # Aggregate to cell level (without model) for summary
            if model_key:
                cell_summary_cols = cell_key + ["period"] if cell_key else ["period"]
                cell_agg = merged.groupby(cell_summary_cols, as_index=False).agg({
                    "sales": "sum", "sales_0": "sum", "dV": "sum",
                    "dM": "first", "market_growth_effect": "sum",
                    "share_shift_effect": "sum", "interaction_effect": "sum",
                })
            else:
                cell_agg = merged

            cells = []
            for _, row in cell_agg.iterrows():
                d: dict[str, Any] = {"level": level_idx}
                for c in cell_key:
                    d[c] = row.get(c)
                d["sales"] = row.get("sales")
                d["sales_0"] = row.get("sales_0")
                d["dV"] = row.get("dV")
                d["dM"] = row.get("dM")
                d["market_growth_effect"] = row.get("market_growth_effect")
                d["share_shift_effect"] = row.get("share_shift_effect")
                d["interaction_effect"] = row.get("interaction_effect")
                cells.append(d)
            cells.sort(key=lambda r: abs(r.get("share_shift_effect") or 0), reverse=True)

            levels.append({
                "level": level_idx,
                "label": "Total Market" if level_idx == 0 else " × ".join(available_hierarchy[:level_idx]),
                "cell_dims": cell_dims,
                "cell_count": len(cells),
                "cells": cells[:30],
            })

        return {
            "base_period": base,
            "target_period": tgt,
            "hierarchy": available_hierarchy,
            "levels": levels,
        }

    return _cached_or_compute(key, _compute)


def compute_drilldown(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    scope_filters: list[dict[str, str]] | None = None,
    base_period: str | None = None,
    top_n: int = 20,
) -> dict[str, Any]:
    """
    Interactive drill-down analysis. Given a scope (country + filters),
    return the next level of decomposition.

    scope_filters: [{"dim": "segment", "value": "SUV-A0"}, {"dim": "registration_type", "value": "Business"}]
    Returns:
      - scope_path: the current drill-down path
      - scope_summary: total sales, dV, dM, market state (growth/decline/stable)
      - available_dims: which dimensions can be drilled next
      - cells: the current level's cellular decomposition
      - models: if at deepest level, model-level winners/losers
    """
    if scope_filters is None:
        scope_filters = []

    sf_key = "|".join(f"{f['dim']}={f['value']}" for f in scope_filters) if scope_filters else "_root"
    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        scope=sf_key, base_period=base_period)
    key = _cache_key("drilldown", **cache_kwargs)

    def _compute():
        fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types)

        used_dims = {f["dim"] for f in scope_filters if f.get("dim") in fact.columns}
        fact = _apply_scope_filters(fact, scope_filters)

        if len(fact) == 0:
            return {"error": "No data for selected scope"}

        periods = sorted(fact["period"].unique())
        if not periods:
            return {"error": "No periods in filtered data"}
        tgt = target_period or periods[-1]
        base = base_period or _shift_period(tgt, -12, periods) or periods[0]

        # Default drill hierarchy: coarse → fine
        drill_hierarchy = ["segment", "make", "powertrain", "registration_type", "drive_type"]
        available_dims = [d for d in drill_hierarchy if d in fact.columns and d not in used_dims]

        # Next dimension to drill into
        drill_dim = available_dims[0] if available_dims else None

        # Model level: no more hierarchy dims left, or model explicitly used as filter
        is_model_level = drill_dim is None

        scope_path = [{"dim": f["dim"], "value": f["value"]} for f in scope_filters]

        # Build current-level aggregation
        if not is_model_level and drill_dim:
            cell_key = [drill_dim] if drill_dim not in used_dims else []

            if cell_key:
                cell_agg = fact.groupby(cell_key + ["period"], as_index=False)["sales"].sum()
                # Merge base and target
                cell_base = cell_agg[cell_agg["period"] == base]
                cell_tgt = cell_agg[cell_agg["period"] == tgt]
                merged = cell_tgt.merge(cell_base, on=cell_key, how="outer", suffixes=("", "_0"))
                merged["sales"] = merged["sales"].fillna(0.0)
                merged["sales_0"] = merged["sales_0"].fillna(0.0)
                merged["dV"] = merged["sales"] - merged["sales_0"]
                merged["yoy_pct"] = np.where(merged["sales_0"] > 0, merged["dV"] / merged["sales_0"], 0.0)

                # Market state classification
                merged["market_state"] = merged["dV"].apply(
                    lambda x: "growth" if x > 5 else ("decline" if x < -5 else "stable")
                )

                merged = merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)
                merged["_abs"] = merged["dV"].abs()
                merged = merged.nlargest(top_n, "_abs").drop(columns=["_abs"])

                cells: list[dict[str, Any]] = []
                for _, row in merged.iterrows():
                    d: dict[str, Any] = {"label": str(row.get(drill_dim, "N/A"))}
                    d["sales"] = row["sales"]
                    d["sales_0"] = row["sales_0"]
                    d["dV"] = row["dV"]
                    d["yoy_pct"] = row["yoy_pct"]
                    d["market_state"] = row.get("market_state", "stable")
                    cells.append(d)

                # Scope summary
                total_tgt = fact[fact["period"] == tgt]["sales"].sum()
                total_base = fact[fact["period"] == base]["sales"].sum()
                dV_total = total_tgt - total_base

                return {
                    "scope_path": scope_path,
                    "scope_summary": {
                        "total_sales": float(total_tgt),
                        "total_sales_0": float(total_base),
                        "dV": float(dV_total),
                        "yoy_pct": float(dV_total / total_base) if total_base > 0 else 0.0,
                        "market_state": "growth" if dV_total > 5 else ("decline" if dV_total < -5 else "stable"),
                    },
                    "drill_dim": drill_dim,
                    "available_dims": available_dims,
                    "is_model_level": False,
                    "cells": cells,
                    "base_period": base,
                    "target_period": tgt,
                }
            else:
                return {"error": "No drill dimension available"}
        else:
            # Model level: compute shift-share for models within current scope
            model_key = [c for c in ["model", "segment", "make"] if c in fact.columns and c not in ["model"]]
            model_agg = fact.groupby(["model", "period"], as_index=False)["sales"].sum()
            market_total = fact.groupby("period", as_index=False)["sales"].sum().rename(columns={"sales": "M"})
            model_agg = model_agg.merge(market_total, on="period", how="left")
            model_agg["s"] = np.where(model_agg["M"] > 0, model_agg["sales"] / model_agg["M"], 0.0)

            base_data = model_agg[model_agg["period"] == base]
            tgt_data = model_agg[model_agg["period"] == tgt]
            merged = tgt_data.merge(base_data[["model", "sales", "M", "s"]], on="model", how="outer", suffixes=("", "_0"))
            for col in ["sales_0", "M_0", "s_0"]:
                merged[col] = merged[col].fillna(0.0)
            merged["sales"] = merged["sales"].fillna(0.0)
            merged["M"] = merged["M"].fillna(0.0)
            merged["s"] = merged["s"].fillna(0.0)

            merged["dV"] = merged["sales"] - merged["sales_0"]
            merged["dM"] = merged["M"] - merged["M_0"]
            merged["ds"] = merged["s"] - merged["s_0"]
            merged["market_growth_effect"] = merged["s_0"] * merged["dM"]
            merged["share_shift_effect"] = merged["M_0"] * merged["ds"]
            merged["interaction_effect"] = merged["ds"] * merged["dM"]
            merged = merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)

            winners = merged[merged["share_shift_effect"] > 0].nlargest(top_n, "share_shift_effect")
            losers = merged[merged["share_shift_effect"] < 0].nsmallest(top_n, "share_shift_effect")

            def _model_row(r):
                return {
                    "model": str(r.get("model", "?")),
                    "sales": float(r.get("sales", 0)),
                    "sales_0": float(r.get("sales_0", 0)),
                    "dV": float(r.get("dV", 0)),
                    "market_growth_effect": float(r.get("market_growth_effect", 0)),
                    "share_shift_effect": float(r.get("share_shift_effect", 0)),
                    "interaction_effect": float(r.get("interaction_effect", 0)),
                }

            total_tgt = fact[fact["period"] == tgt]["sales"].sum()
            total_base = fact[fact["period"] == base]["sales"].sum()

            return {
                "scope_path": scope_path,
                "scope_summary": {
                    "total_sales": float(total_tgt),
                    "total_sales_0": float(total_base),
                    "dV": float(total_tgt - total_base),
                    "yoy_pct": float((total_tgt - total_base) / total_base) if total_base > 0 else 0.0,
                    "market_state": "growth" if (total_tgt - total_base) > 5 else ("decline" if (total_tgt - total_base) < -5 else "stable"),
                },
                "drill_dim": "model",
                "available_dims": available_dims,
                "is_model_level": True,
                "winners": [_model_row(r) for _, r in winners.iterrows()],
                "losers": [_model_row(r) for _, r in losers.iterrows()],
                "base_period": base,
                "target_period": tgt,
            }

    return _cached_or_compute(key, _compute)


def compute_transfer_mart(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    scope_filters: list[dict[str, str]] | None = None,
    base_period: str | None = None,
    sales_mode: str = "month",
    top_n: int = 25,
) -> dict[str, Any]:
    """
    One-stop transfer mart: pre-aggregated table with all decomposition components
    for a one-page transfer analysis dashboard.

    Returns:
      - scope_summary: market state, total dV, YoY
      - market_waterfall: total ΔV decomposed into market carryover + mix effects + pure share shift
      - models: per-model decomposition (market_carryover, channel_mix, drive_mix, powertrain_mix, pure_share_shift, interaction, donors, recipients)
      - channel_drive_heatmap: net share shift per channel×drive cell
      - powertrain_origin_breakdown: share shift per powertrain and origin
      - momentum: recent months share momentum per top model
    """
    if scope_filters is None:
        scope_filters = []
    if sales_mode not in {"month", "ytd", "rolling12"}:
        sales_mode = "month"

    sf_key = "|".join(f"{f['dim']}={f['value']}" for f in scope_filters) if scope_filters else "_root"
    cache_kwargs = dict(country=country, target_period=target_period,
                        fuel_types=",".join(fuel_types or []),
                        segments=",".join(segments or []),
                        scope=sf_key, base=base_period, sales_mode=sales_mode, top_n=top_n)
    key = _cache_key("transfer_mart", **cache_kwargs)

    def _compute():
        raw_fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)
        selected_values = _scope_filter_values(scope_filters)
        source_fact = _apply_scope_filters(raw_fact, scope_filters)
        if len(source_fact) == 0:
            return {"error": "No data for selected scope"}

        periods = sorted(source_fact["period"].unique())
        if len(periods) < 2:
            return {"error": "Need at least 2 periods"}
        tgt = target_period or periods[-1]
        base = base_period or _shift_period(tgt, -12, periods) or periods[0]
        fact = _aggregate_fact_for_sales_mode(source_fact, periods, tgt, base, sales_mode)

        # ── Scope summary ──
        total_tgt = float(fact[fact["period"] == tgt]["sales"].sum())
        total_base = float(fact[fact["period"] == base]["sales"].sum())
        dM = total_tgt - total_base
        threshold = abs(total_base) * 0.02 if total_base > 0 else 1.0
        market_state = "growth" if dM > threshold else ("decline" if dM < -threshold else "stable")

        scope_summary = {
            "total_sales_tgt": total_tgt, "total_sales_base": total_base,
            "dM": dM, "yoy_pct": float(dM / total_base) if total_base > 0 else 0.0,
            "market_state": market_state,
        }

        # ── Market waterfall (total decomposition) ──
        # Total = market carryover + channel mix + drive mix + powertrain mix + pure share + interaction
        # For total level, market carryover = total_base * (total growth rate)
        overall_growth = dM
        market_carryover_total = total_base * (dM / total_base) if total_base > 0 else 0.0

        # Channel mix: how much of dM comes from Business vs Private share changes
        chan_available = "registration_type" in fact.columns and fact["registration_type"].nunique() > 1
        drive_available = "drive_type" in fact.columns and fact["drive_type"].nunique() > 1
        pwr_available = "powertrain" in fact.columns and fact["powertrain"].nunique() > 1

        waterfall_items: list[dict[str, Any]] = [
            {"label": "Market Carryover", "value": market_carryover_total, "kind": "market"},
        ]

        # ── Per-model decomposition ──
        model_market = fact.groupby(["model", "period"], as_index=False)["sales"].sum()
        total_by_period = fact.groupby("period", as_index=False)["sales"].sum().rename(columns={"sales": "M"})
        model_market = model_market.merge(total_by_period, on="period", how="left")
        model_market["share"] = np.where(model_market["M"] > 0, model_market["sales"] / model_market["M"], 0.0)

        model_base = model_market[model_market["period"] == base].copy()
        model_tgt = model_market[model_market["period"] == tgt].copy()

        merged = model_tgt.merge(
            model_base[["model", "sales", "M", "share"]],
            on="model", how="outer", suffixes=("", "_0"),
        )
        for col in ["sales_0", "M_0", "share_0"]:
            merged[col] = merged[col].fillna(0.0)
        merged["sales"] = merged["sales"].fillna(0.0)
        merged["M"] = merged["M"].fillna(0.0)
        merged["share"] = merged["share"].fillna(0.0)

        merged["dV"] = merged["sales"] - merged["sales_0"]
        merged["dM"] = merged["M"] - merged["M_0"]
        merged["ds"] = merged["share"] - merged["share_0"]
        merged["market_carryover"] = merged["share_0"] * merged["dM"]
        merged["pure_share_shift"] = merged["M_0"] * merged["ds"]
        merged["interaction"] = merged["ds"] * merged["dM"]
        merged = merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)

        # Channel/drive/powertrain mix contributions per model
        if chan_available:
            chan_data = fact.groupby(["model", "registration_type", "period"], as_index=False)["sales"].sum()
            chan_total = fact.groupby(["registration_type", "period"], as_index=False)["sales"].sum().rename(columns={"sales": "C"})
            chan_data = chan_data.merge(chan_total, on=["registration_type", "period"], how="left")
            chan_data["chan_share"] = np.where(chan_data["C"] > 0, chan_data["sales"] / chan_data["C"], 0.0)

            chan_base = chan_data[chan_data["period"] == base]
            chan_tgt = chan_data[chan_data["period"] == tgt]
            chan_merged = chan_tgt.merge(
                chan_base[["model", "registration_type", "chan_share", "C"]],
                on=["model", "registration_type"], how="outer", suffixes=("", "_0"),
            )
            for c in ["chan_share_0", "C_0"]:
                chan_merged[c] = chan_merged[c].fillna(0.0)
            chan_merged["chan_share"] = chan_merged["chan_share"].fillna(0.0)
            chan_merged["C"] = chan_merged["C"].fillna(0.0)
            chan_merged["chan_mix_effect"] = chan_merged["C_0"] * (chan_merged["chan_share"] - chan_merged["chan_share_0"])
            chan_mix_by_model = chan_merged.groupby("model", as_index=False)["chan_mix_effect"].sum().fillna(0.0)
            merged = merged.merge(chan_mix_by_model, on="model", how="left")
            merged["channel_mix"] = merged["chan_mix_effect"].fillna(0.0)
            merged = merged.drop(columns=["chan_mix_effect"], errors="ignore")
            waterfall_items.append({"label": "Channel Mix", "value": float(merged["channel_mix"].sum()), "kind": "mix"})
        else:
            merged["channel_mix"] = 0.0

        if drive_available:
            drive_data = fact.groupby(["model", "drive_type", "period"], as_index=False)["sales"].sum()
            drive_total = fact.groupby(["drive_type", "period"], as_index=False)["sales"].sum().rename(columns={"sales": "D"})
            drive_data = drive_data.merge(drive_total, on=["drive_type", "period"], how="left")
            drive_data["drive_share"] = np.where(drive_data["D"] > 0, drive_data["sales"] / drive_data["D"], 0.0)
            drive_base = drive_data[drive_data["period"] == base]
            drive_tgt = drive_data[drive_data["period"] == tgt]
            drive_merged = drive_tgt.merge(
                drive_base[["model", "drive_type", "drive_share", "D"]],
                on=["model", "drive_type"], how="outer", suffixes=("", "_0"),
            )
            for c in ["drive_share_0", "D_0"]:
                drive_merged[c] = drive_merged[c].fillna(0.0)
            drive_merged["drive_share"] = drive_merged["drive_share"].fillna(0.0)
            drive_merged["D"] = drive_merged["D"].fillna(0.0)
            drive_merged["drive_mix_effect"] = drive_merged["D_0"] * (drive_merged["drive_share"] - drive_merged["drive_share_0"])
            drive_mix_by_model = drive_merged.groupby("model", as_index=False)["drive_mix_effect"].sum().fillna(0.0)
            merged = merged.merge(drive_mix_by_model, on="model", how="left")
            merged["drive_mix"] = merged["drive_mix_effect"].fillna(0.0)
            merged = merged.drop(columns=["drive_mix_effect"], errors="ignore")
            waterfall_items.append({"label": "Drive Mix", "value": float(merged["drive_mix"].sum()), "kind": "mix"})
        else:
            merged["drive_mix"] = 0.0

        if pwr_available:
            pwr_data = fact.groupby(["model", "powertrain", "period"], as_index=False)["sales"].sum()
            pwr_total = fact.groupby(["powertrain", "period"], as_index=False)["sales"].sum().rename(columns={"sales": "P"})
            pwr_data = pwr_data.merge(pwr_total, on=["powertrain", "period"], how="left")
            pwr_data["pwr_share"] = np.where(pwr_data["P"] > 0, pwr_data["sales"] / pwr_data["P"], 0.0)
            pwr_base = pwr_data[pwr_data["period"] == base]
            pwr_tgt = pwr_data[pwr_data["period"] == tgt]
            pwr_merged = pwr_tgt.merge(
                pwr_base[["model", "powertrain", "pwr_share", "P"]],
                on=["model", "powertrain"], how="outer", suffixes=("", "_0"),
            )
            for c in ["pwr_share_0", "P_0"]:
                pwr_merged[c] = pwr_merged[c].fillna(0.0)
            pwr_merged["pwr_share"] = pwr_merged["pwr_share"].fillna(0.0)
            pwr_merged["P"] = pwr_merged["P"].fillna(0.0)
            pwr_merged["pwr_mix_effect"] = pwr_merged["P_0"] * (pwr_merged["pwr_share"] - pwr_merged["pwr_share_0"])
            pwr_mix_by_model = pwr_merged.groupby("model", as_index=False)["pwr_mix_effect"].sum().fillna(0.0)
            merged = merged.merge(pwr_mix_by_model, on="model", how="left")
            merged["powertrain_mix"] = merged["pwr_mix_effect"].fillna(0.0)
            merged = merged.drop(columns=["pwr_mix_effect"], errors="ignore")
            waterfall_items.append({"label": "Powertrain Mix", "value": float(merged["powertrain_mix"].sum()), "kind": "mix"})
        else:
            merged["powertrain_mix"] = 0.0

        waterfall_items.append({"label": "Pure Share Shift", "value": float(merged["pure_share_shift"].sum()), "kind": "share"})
        waterfall_items.append({"label": "Interaction", "value": float(merged["interaction"].sum()), "kind": "interaction"})

        # ── Model list ──
        merged["net_gain_loss"] = merged["dV"]
        merged["_abs_net"] = merged["net_gain_loss"].abs()
        top_models = merged.nlargest(top_n, "_abs_net")
        winners_all = merged[merged["pure_share_shift"] > 0].nlargest(top_n, "pure_share_shift")
        losers_all = merged[merged["pure_share_shift"] < 0].nsmallest(top_n, "pure_share_shift")

        def _model_row(r, donors=None, recipients=None):
            share_change = float(r.get("ds", 0))
            return {
                "model": str(r.get("model", "?")),
                "sales_tgt": float(r.get("sales", 0)),
                "sales_base": float(r.get("sales_0", 0)),
                "dV": float(r.get("dV", 0)),
                "share_tgt": float(r.get("share", 0)),
                "share_base": float(r.get("share_0", 0)),
                "share_change": share_change,
                "market_carryover": float(r.get("market_carryover", 0)),
                "channel_mix": float(r.get("channel_mix", 0)),
                "drive_mix": float(r.get("drive_mix", 0)),
                "powertrain_mix": float(r.get("powertrain_mix", 0)),
                "pure_share_shift": float(r.get("pure_share_shift", 0)),
                "interaction": float(r.get("interaction", 0)),
                "donors": donors or [],
                "recipients": recipients or [],
                "resilience": "strong" if share_change > 0.01 else ("weak" if share_change < -0.01 else "neutral"),
            }

        # D/R estimation: within same scope, pair winners with losers proportionally
        winner_models = [_model_row(r) for _, r in winners_all.iterrows()]
        loser_models = [_model_row(r) for _, r in losers_all.iterrows()]
        total_loss = sum(abs(l["pure_share_shift"]) for l in loser_models)
        if total_loss > 0:
            for w in winner_models:
                w["donors"] = [
                    {"model": l["model"], "estimated_flow": round(w["pure_share_shift"] * abs(l["pure_share_shift"]) / total_loss, 1)}
                    for l in loser_models
                    if w["pure_share_shift"] * abs(l["pure_share_shift"]) / total_loss > 0.5
                ][:5]
            for l in loser_models:
                l["recipients"] = [
                    {"model": w["model"], "estimated_flow": round(w["pure_share_shift"] * abs(l["pure_share_shift"]) / total_loss, 1)}
                    for w in winner_models
                    if w["pure_share_shift"] * abs(l["pure_share_shift"]) / total_loss > 0.5
                ][:5]
        all_models = [_model_row(r) for _, r in top_models.iterrows()]
        all_models.sort(key=lambda m: abs(m["dV"]), reverse=True)

        # ── Channel × Drive heatmap ──
        heatmap: list[dict[str, Any]] = []
        if chan_available and drive_available:
            cd_data = fact.groupby(["registration_type", "drive_type", "period"], as_index=False)["sales"].sum()
            cd_base = cd_data[cd_data["period"] == base]
            cd_tgt = cd_data[cd_data["period"] == tgt]
            cd_merged = cd_tgt.merge(cd_base, on=["registration_type", "drive_type"], how="outer", suffixes=("", "_0"))
            cd_merged["sales"] = cd_merged["sales"].fillna(0.0)
            cd_merged["sales_0"] = cd_merged["sales_0"].fillna(0.0)
            cd_merged["net_share_shift"] = cd_merged["sales"] - cd_merged["sales_0"]
            cd_merged = cd_merged.fillna(0.0)
            for _, row in cd_merged.iterrows():
                heatmap.append({
                    "channel": str(row.get("registration_type", "?")),
                    "drive": str(row.get("drive_type", "?")),
                    "net_shift": float(row.get("net_share_shift", 0)),
                })

        # ── Powertrain × Origin breakdown ──
        pwr_origin: list[dict[str, Any]] = []
        if "powertrain" in fact.columns and "origin" in fact.columns:
            po_data = fact.groupby(["powertrain", "origin", "period"], as_index=False)["sales"].sum()
            po_base = po_data[po_data["period"] == base]
            po_tgt = po_data[po_data["period"] == tgt]
            po_merged = po_tgt.merge(po_base, on=["powertrain", "origin"], how="outer", suffixes=("", "_0"))
            po_merged["sales"] = po_merged["sales"].fillna(0.0)
            po_merged["sales_0"] = po_merged["sales_0"].fillna(0.0)
            po_merged["shift"] = po_merged["sales"] - po_merged["sales_0"]
            po_merged = po_merged.fillna(0.0)
            for _, row in po_merged.iterrows():
                pwr_origin.append({
                    "powertrain": str(row.get("powertrain", "?")),
                    "origin": str(row.get("origin", "?")),
                    "shift": float(row.get("shift", 0)),
                    "sales": float(row.get("sales", 0)),
                })

        # ── Momentum (last 3 periods) ──
        momentum: list[dict[str, Any]] = []
        recent_periods = periods[-4:] if len(periods) >= 4 else periods
        if len(recent_periods) >= 2:
            for _, r in top_models.head(12).iterrows():
                model_name = r["model"]
                model_ts = source_fact[source_fact["model"] == model_name].groupby("period", as_index=False)["sales"].sum()
                model_ts = model_ts[model_ts["period"].isin(recent_periods)]
                model_ts = model_ts.sort_values("period")
                shares = []
                for p in recent_periods:
                    total = source_fact[source_fact["period"] == p]["sales"].sum()
                    m_sales = model_ts[model_ts["period"] == p]["sales"].sum()
                    shares.append(float(m_sales / total) if total > 0 else 0.0)
                if len(shares) >= 2:
                    slope = (shares[-1] - shares[0]) / max(len(shares) - 1, 1)
                    momentum.append({
                        "model": model_name,
                        "share_slope": round(slope, 6),
                        "recent_shares": shares,
                        "trend": "rising" if slope > 0.0005 else ("falling" if slope < -0.0005 else "flat"),
                    })

        # ── Channel timeseries (stacked bar: Business/Private per period) ──
        channel_ts: list[dict[str, Any]] = []
        if chan_available:
            ch_ts = source_fact.groupby(["period", "registration_type"], as_index=False)["sales"].sum()
            ch_ts = ch_ts.sort_values("period")
            for _, row in ch_ts.iterrows():
                channel_ts.append({
                    "period": str(row["period"]),
                    "channel": str(row.get("registration_type", "?")),
                    "volume": float(row["sales"]),
                })

        # ── Powertrain timeseries (stacked bar: BEV/HEV/PHEV/ICE per period) ──
        pwt_ts: list[dict[str, Any]] = []
        if "powertrain" in source_fact.columns:
            pt_ts = source_fact.groupby(["period", "powertrain"], as_index=False)["sales"].sum()
            pt_ts = pt_ts.sort_values("period")
            total_by_period = source_fact.groupby("period")["sales"].sum().to_dict()
            for _, row in pt_ts.iterrows():
                p = str(row["period"])
                total = total_by_period.get(p, 1)
                pwt_ts.append({
                    "period": p,
                    "powertrain": str(row.get("powertrain", "?")),
                    "volume": float(row["sales"]),
                    "share": float(row["sales"] / total) if total > 0 else 0.0,
                })

        # ── Model share timeseries (for sparklines) ──
        model_ts: list[dict[str, Any]] = []
        top_model_names = [m["model"] for m in all_models[:15]]
        if top_model_names:
            total_by_period = source_fact.groupby("period")["sales"].sum()
            for mn in top_model_names:
                mf = source_fact[source_fact["model"] == mn].groupby("period")["sales"].sum()
                shares = []
                for p in sorted(periods):
                    ts = total_by_period.get(p, 1)
                    ms = mf.get(p, 0)
                    shares.append({"period": p, "share": round(float(ms / ts) * 100, 2) if ts > 0 else 0})
                if shares:
                    model_ts.append({"model": mn, "shares": shares})

        return {
            "base_period": base,
            "target_period": tgt,
            "sales_mode": sales_mode,
            "scope_summary": scope_summary,
            "market_waterfall": waterfall_items,
            "winners": winner_models,
            "losers": loser_models,
            "models": all_models,
            "channel_drive_heatmap": heatmap,
            "powertrain_origin_breakdown": pwr_origin,
            "momentum": momentum,
            "channel_timeseries": channel_ts,
            "powertrain_timeseries": pwt_ts,
            "model_timeseries": model_ts,
        }

    return _cached_or_compute(key, _compute)


def _model_metrics_from_fact(fact: pd.DataFrame, base: str, tgt: str) -> pd.DataFrame:
    model_market = fact.groupby(["model", "period"], as_index=False)["sales"].sum()
    total_by_period = fact.groupby("period", as_index=False)["sales"].sum().rename(columns={"sales": "M"})
    model_market = model_market.merge(total_by_period, on="period", how="left")
    model_market["share"] = np.where(model_market["M"] > 0, model_market["sales"] / model_market["M"], 0.0)

    model_base = model_market[model_market["period"] == base].copy()
    model_tgt = model_market[model_market["period"] == tgt].copy()
    merged = model_tgt.merge(
        model_base[["model", "sales", "M", "share"]],
        on="model", how="outer", suffixes=("", "_0"),
    )
    for col in ["sales_0", "M_0", "share_0"]:
        merged[col] = merged[col].fillna(0.0)
    merged["sales"] = merged["sales"].fillna(0.0)
    merged["M"] = merged["M"].fillna(0.0)
    merged["share"] = merged["share"].fillna(0.0)
    merged["dV"] = merged["sales"] - merged["sales_0"]
    merged["dM"] = merged["M"] - merged["M_0"]
    merged["ds"] = merged["share"] - merged["share_0"]
    merged["market_carryover"] = merged["share_0"] * merged["dM"]
    merged["pure_share_shift"] = merged["M_0"] * merged["ds"]
    merged["interaction"] = merged["ds"] * merged["dM"]
    return merged.replace([np.inf, -np.inf], 0.0).fillna(0.0)


def _dominant_model_profiles(fact: pd.DataFrame, base: str, tgt: str) -> dict[str, dict[str, Any]]:
    profile_dims = ["make", "segment", "body_type", "powertrain", "registration_type", "drive_type", "origin"]
    available_dims = [dim for dim in profile_dims if dim in fact.columns]
    numeric_dims = [field for field in NUMERIC_MATCH_WEIGHTS if field in fact.columns]
    profiles: dict[str, dict[str, Any]] = {
        str(model): {} for model in fact["model"].dropna().unique().tolist()
    }

    def _fill_from_period(period: str) -> None:
        period_fact = fact[fact["period"] == period]
        if len(period_fact) == 0:
            return
        for dim in available_dims:
            ranked = (
                period_fact.groupby(["model", dim], as_index=False, dropna=False)["sales"]
                .sum()
                .sort_values(["model", "sales"], ascending=[True, False])
            )
            ranked = ranked.drop_duplicates("model")
            for _, row in ranked.iterrows():
                model = str(row.get("model", ""))
                value = row.get(dim)
                if not model or pd.isna(value):
                    continue
                profiles.setdefault(model, {})
                if dim not in profiles[model]:
                    profiles[model][dim] = str(value)

    def _weighted_average(group: pd.DataFrame, field: str) -> float:
        weights = pd.to_numeric(group["sales"], errors="coerce").fillna(0.0).clip(lower=0.0)
        if float(weights.sum()) > 0:
            return float(np.average(group[field], weights=weights))
        return float(group[field].median())

    def _fill_numeric_from_period(period: str) -> None:
        period_fact = fact[fact["period"] == period]
        if len(period_fact) == 0:
            return
        for field in numeric_dims:
            numeric = period_fact[["model", "sales", field]].copy()
            numeric[field] = pd.to_numeric(numeric[field], errors="coerce")
            numeric = numeric.dropna(subset=[field])
            if field not in {"co2_emission", "fuel_consumption"}:
                numeric = numeric[numeric[field] > 0]
            if len(numeric) == 0:
                continue
            for model, group in numeric.groupby("model", dropna=False):
                value = _weighted_average(group, field)
                model_key = str(model)
                if not model_key or not np.isfinite(value):
                    continue
                profiles.setdefault(model_key, {})
                if field not in profiles[model_key]:
                    profiles[model_key][field] = round(float(value), 2)

    _fill_from_period(tgt)
    _fill_from_period(base)
    _fill_numeric_from_period(tgt)
    _fill_numeric_from_period(base)
    return profiles


def _model_channel_timeseries(source_fact: pd.DataFrame, models: list[str]) -> list[dict[str, Any]]:
    if "registration_type" not in source_fact.columns or not models:
        return []
    model_fact = source_fact[source_fact["model"].isin(models)]
    if len(model_fact) == 0:
        return []

    channel_rows = (
        model_fact.groupby(["model", "period", "registration_type"], as_index=False)["sales"]
        .sum()
        .sort_values(["model", "period", "registration_type"])
    )
    total_rows = (
        model_fact.groupby(["model", "period"], as_index=False)["sales"]
        .sum()
        .rename(columns={"sales": "total_sales"})
    )
    channel_rows = channel_rows.merge(total_rows, on=["model", "period"], how="left")
    channel_rows["share"] = np.where(
        channel_rows["total_sales"] > 0,
        channel_rows["sales"] / channel_rows["total_sales"],
        0.0,
    )

    rows: list[dict[str, Any]] = []
    for _, row in channel_rows.iterrows():
        rows.append({
            "model": str(row.get("model", "")),
            "period": str(row.get("period", "")),
            "channel": str(row.get("registration_type", "")),
            "volume": float(row.get("sales", 0.0)),
            "total_volume": float(row.get("total_sales", 0.0)),
            "share": float(row.get("share", 0.0)),
        })
    return rows


def _profile_channel_timeseries(source_fact: pd.DataFrame, label: str) -> list[dict[str, Any]]:
    if "registration_type" not in source_fact.columns or len(source_fact) == 0:
        return []
    channel_rows = (
        source_fact.groupby(["period", "registration_type"], as_index=False)["sales"]
        .sum()
        .sort_values(["period", "registration_type"])
    )
    total_rows = (
        source_fact.groupby("period", as_index=False)["sales"]
        .sum()
        .rename(columns={"sales": "total_sales"})
    )
    channel_rows = channel_rows.merge(total_rows, on="period", how="left")
    channel_rows["share"] = np.where(
        channel_rows["total_sales"] > 0,
        channel_rows["sales"] / channel_rows["total_sales"],
        0.0,
    )
    rows: list[dict[str, Any]] = []
    for _, row in channel_rows.iterrows():
        rows.append({
            "model": label,
            "period": str(row.get("period", "")),
            "channel": str(row.get("registration_type", "")),
            "volume": float(row.get("sales", 0.0)),
            "total_volume": float(row.get("total_sales", 0.0)),
            "share": float(row.get("share", 0.0)),
        })
    return rows


def _profile_label(selected_values: dict[str, set[str]]) -> str:
    parts: list[str] = []
    for dim in PROFILE_FILTER_DIMS:
        values = sorted(selected_values.get(dim, set()))
        if values:
            parts.append("/".join(values[:3]) + ("+" if len(values) > 3 else ""))
    return "Selected Profile" if not parts else "Profile: " + " · ".join(parts[:4])


def compute_competitor_set(
    country: str | None = None,
    target_period: str | None = None,
    time_range: dict[str, str] | None = None,
    fuel_types: list[str] | None = None,
    segments: list[str] | None = None,
    scope_filters: list[dict[str, str]] | None = None,
    base_period: str | None = None,
    sales_mode: str = "month",
    target_model: str | None = None,
    profile_specs: dict[str, Any] | None = None,
    top_n: int = 12,
) -> dict[str, Any]:
    """Build a product-centric competitive set using model profile similarity."""
    if scope_filters is None:
        scope_filters = []
    if sales_mode not in {"month", "ytd", "rolling12"}:
        sales_mode = "month"

    sf_key = "|".join(f"{f['dim']}={f['value']}" for f in scope_filters) if scope_filters else "_root"
    normalized_specs = _normalize_profile_specs(profile_specs)
    spec_key = "|".join(f"{field}={normalized_specs[field]:.4g}" for field in sorted(normalized_specs))
    cache_kwargs = dict(
        country=country,
        target_period=target_period,
        fuel_types=",".join(fuel_types or []),
        segments=",".join(segments or []),
        scope=sf_key,
        base=base_period,
        sales_mode=sales_mode,
        target_model=target_model,
        profile_specs=spec_key,
        top_n=top_n,
        time_range=f"{time_range['start']}_{time_range['end']}" if time_range else "",
    )
    key = _cache_key("competitor_set", **cache_kwargs)

    def _compute():
        raw_fact = build_fact_sales_monthly(country=country, fuel_types=fuel_types, segments=segments)
        selected_values = _scope_filter_values(scope_filters)
        source_fact = _apply_scope_filters(raw_fact, scope_filters)
        if len(source_fact) == 0:
            return {"error": "No data for selected scope"}

        periods = sorted(source_fact["period"].unique())
        if len(periods) < 2:
            return {"error": "Need at least 2 periods"}
        tgt = target_period or (time_range["end"] if time_range else periods[-1])
        if tgt not in periods:
            tgt = periods[-1]
        base = base_period or _shift_period(tgt, -12, periods) or periods[0]
        if base not in periods:
            base = _shift_period(tgt, -12, periods) or periods[0]

        fact = _aggregate_fact_for_sales_mode(source_fact, periods, tgt, base, sales_mode)
        metrics = _model_metrics_from_fact(fact, base, tgt)
        if len(metrics) == 0:
            return {"error": "No model metrics for selected scope"}

        metrics_by_model = {str(row.get("model")): row for _, row in metrics.iterrows()}
        profiles = _dominant_model_profiles(fact, base, tgt)
        available_models = sorted(metrics_by_model.keys())
        if not available_models:
            return {"error": "No models for selected scope"}

        selected_model = target_model if target_model in metrics_by_model else None
        profile_mode = selected_model is None
        selected_label = _profile_label(selected_values) if profile_mode else selected_model

        selected_dims = [dim for dim in CATEGORICAL_MATCH_WEIGHTS if selected_values.get(dim)]
        available_numeric_fields = [field for field in NUMERIC_MATCH_WEIGHTS if field in fact.columns]
        if profile_mode:
            target_profile = {
                dim: " / ".join(sorted(selected_values.get(dim, set())))
                for dim in selected_dims
            }
            target_shift = 0.0
        else:
            target_row = metrics_by_model[selected_model]
            target_profile = profiles.get(selected_model, {})
            target_shift = float(target_row.get("pure_share_shift", 0.0))

        for field, value in normalized_specs.items():
            target_profile[field] = round(value, 2)

        target_specs: dict[str, float] = {}
        for field in available_numeric_fields:
            selected_spec = normalized_specs.get(field)
            profile_spec = _numeric_value(target_profile.get(field))
            target_value = selected_spec if selected_spec is not None else profile_spec
            if target_value is not None:
                target_specs[field] = target_value
                target_profile[field] = round(target_value, 2)

        active_categorical_dims = selected_dims or ([] if profile_mode else [dim for dim in CATEGORICAL_MATCH_WEIGHTS if dim in target_profile])
        active_numeric_dims = [field for field in NUMERIC_MATCH_WEIGHTS if field in target_specs]
        active_dims = [*active_categorical_dims, *active_numeric_dims]
        total_weight = (
            sum(CATEGORICAL_MATCH_WEIGHTS[dim] for dim in active_categorical_dims)
            + sum(NUMERIC_MATCH_WEIGHTS[field] for field in active_numeric_dims)
        ) or 1.0

        competitor_rows: list[dict[str, Any]] = []
        for model, row in metrics_by_model.items():
            if not profile_mode and model == selected_model:
                continue
            profile = profiles.get(model, {})
            shared_dims: list[str] = []
            match_evidence: list[dict[str, Any]] = []
            score_sum = 0.0

            for dim in active_categorical_dims:
                candidate_value = profile.get(dim)
                if not candidate_value:
                    continue
                target_values = selected_values.get(dim, set()) if selected_dims else {str(target_profile.get(dim, ""))}
                target_values = {value for value in target_values if value}
                if candidate_value not in target_values:
                    continue
                shared_dims.append(dim)
                score_sum += CATEGORICAL_MATCH_WEIGHTS[dim]
                label = dim.replace("_", " ").title()
                match_evidence.append({
                    "field": dim,
                    "label": label,
                    "target": " / ".join(sorted(target_values)),
                    "candidate": str(candidate_value),
                    "score": 100.0,
                    "detail": f"{label}: same {candidate_value}",
                })

            for field in active_numeric_dims:
                target_value = target_specs[field]
                candidate_value = _numeric_value(profile.get(field))
                if candidate_value is None:
                    continue
                field_score = _numeric_match_score(field, target_value, candidate_value)
                if field_score <= 0:
                    continue
                score_sum += NUMERIC_MATCH_WEIGHTS[field] * field_score
                if field_score >= 0.65:
                    shared_dims.append(field)
                if field_score >= 0.45:
                    label = PRODUCT_SPEC_LABELS[field]
                    match_evidence.append({
                        "field": field,
                        "label": label,
                        "target": round(float(target_value), 2),
                        "candidate": round(float(candidate_value), 2),
                        "score": round(field_score * 100.0, 1),
                        "detail": f"{label}: {_format_spec_value(field, target_value)} vs {_format_spec_value(field, candidate_value)}",
                    })

            similarity = score_sum / total_weight * 100.0
            if similarity <= 0 and active_dims:
                continue

            competitor_shift = float(row.get("pure_share_shift", 0.0))
            role = "adjacent"
            if profile_mode:
                role = "likely_recipient" if competitor_shift >= 0 else "likely_source"
            else:
                if target_shift >= 0 and competitor_shift < 0:
                    role = "likely_source"
                elif target_shift < 0 and competitor_shift > 0:
                    role = "likely_recipient"
                elif target_shift >= 0 and competitor_shift >= 0:
                    role = "co_winner"
                elif target_shift < 0 and competitor_shift < 0:
                    role = "co_loser"

            competitor_rows.append({
                "model": model,
                "make": profile.get("make", ""),
                "profile": profile,
                "sales_tgt": float(row.get("sales", 0.0)),
                "sales_base": float(row.get("sales_0", 0.0)),
                "dV": float(row.get("dV", 0.0)),
                "share_tgt": float(row.get("share", 0.0)),
                "share_base": float(row.get("share_0", 0.0)),
                "share_change": float(row.get("ds", 0.0)),
                "pure_share_shift": competitor_shift,
                "similarity_score": round(similarity, 1),
                "shared_dims": shared_dims,
                "match_evidence": match_evidence,
                "role": role,
                "estimated_flow": 0.0,
            })

        if not profile_mode and target_shift >= 0:
            flow_pool = [
                row for row in competitor_rows
                if row["pure_share_shift"] < 0 and row["similarity_score"] >= 25
            ]
            denominator = sum(abs(row["pure_share_shift"]) * row["similarity_score"] for row in flow_pool)
            for row in flow_pool:
                row["estimated_flow"] = (
                    abs(target_shift) * abs(row["pure_share_shift"]) * row["similarity_score"] / denominator
                    if denominator > 0 else 0.0
                )
        elif not profile_mode:
            flow_pool = [
                row for row in competitor_rows
                if row["pure_share_shift"] > 0 and row["similarity_score"] >= 25
            ]
            denominator = sum(row["pure_share_shift"] * row["similarity_score"] for row in flow_pool)
            for row in flow_pool:
                row["estimated_flow"] = (
                    abs(target_shift) * row["pure_share_shift"] * row["similarity_score"] / denominator
                    if denominator > 0 else 0.0
                )

        def _rank_score(row: dict[str, Any]) -> float:
            return (
                row["estimated_flow"] * 5.0
                + abs(row["pure_share_shift"]) * (row["similarity_score"] / 100.0)
                + abs(row["dV"]) * 0.15
            )

        competitor_rows.sort(key=_rank_score, reverse=True)
        competitors = competitor_rows[:top_n]

        battle_flows = []
        if not profile_mode:
            for row in competitors:
                if row["estimated_flow"] <= 0:
                    continue
                if target_shift >= 0:
                    source = row["model"]
                    target = selected_model
                else:
                    source = selected_model
                    target = row["model"]
                battle_flows.append({
                    "source": source,
                    "target": target,
                    "value": round(float(row["estimated_flow"]), 1),
                    "similarity_score": row["similarity_score"],
                    "reason": ", ".join(evidence.get("detail", evidence.get("field", "")) for evidence in row.get("match_evidence", [])[:3]),
                })

        if profile_mode:
            total_tgt = float(fact[fact["period"] == tgt]["sales"].sum())
            total_base = float(fact[fact["period"] == base]["sales"].sum())
            target_metrics = {
                "model": selected_label,
                "make": "",
                "profile": target_profile,
                "sales_tgt": total_tgt,
                "sales_base": total_base,
                "dV": total_tgt - total_base,
                "share_tgt": 1.0,
                "share_base": 1.0,
                "share_change": 0.0,
                "pure_share_shift": 0.0,
                "similarity_score": 100.0 if active_dims else 0.0,
                "shared_dims": active_dims,
                "match_evidence": [],
                "role": "target",
                "estimated_flow": 0.0,
            }
        else:
            target_row = metrics_by_model[selected_model]
            target_metrics = {
                "model": selected_model,
                "make": target_profile.get("make", ""),
                "profile": target_profile,
                "sales_tgt": float(target_row.get("sales", 0.0)),
                "sales_base": float(target_row.get("sales_0", 0.0)),
                "dV": float(target_row.get("dV", 0.0)),
                "share_tgt": float(target_row.get("share", 0.0)),
                "share_base": float(target_row.get("share_0", 0.0)),
                "share_change": float(target_row.get("ds", 0.0)),
                "pure_share_shift": target_shift,
                "similarity_score": 100.0,
                "shared_dims": active_dims,
                "match_evidence": [],
                "role": "target",
                "estimated_flow": 0.0,
            }

        model_options = (
            metrics.assign(_abs_shift=metrics["pure_share_shift"].abs())
            .sort_values(["_abs_shift", "sales"], ascending=[False, False])
            .head(80)["model"]
            .astype(str)
            .tolist()
        )
        chart_models = ([] if profile_mode else [selected_model]) + [row["model"] for row in competitors[:5]]
        model_channel_timeseries = _model_channel_timeseries(source_fact, chart_models)
        if profile_mode:
            model_channel_timeseries = _profile_channel_timeseries(source_fact, selected_label) + model_channel_timeseries

        return {
            "base_period": base,
            "target_period": tgt,
            "sales_mode": sales_mode,
            "analysis_mode": "profile" if profile_mode else "target_model",
            "target_model": selected_label,
            "target": target_metrics,
            "competitors": competitors,
            "battle_flows": battle_flows,
            "profile_dimensions": active_dims,
            "model_options": model_options,
            "model_channel_timeseries": model_channel_timeseries,
            "scope_model_count": len(metrics),
        }

    return _cached_or_compute(key, _compute)


def clear_advanced_analysis_cache() -> dict[str, Any]:
    with _cache_lock:
        count = len(_cache)
        _cache.clear()
    return {"cleared": count}
