from uuid import uuid4
import io

import numpy as np
import pandas as pd

from app.core.config import DEFAULT_GROUP_BY
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


def _year_columns(columns: list[str]) -> list[str]:
    return sorted(c for c in columns if len(c) == 4 and c.isdigit())


_MONTH_ORDER = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
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
    return {
        "column": column,
        "options": repo.load_distinct_options(column, filters),
        "rowCount": repo.count_rows(filters),
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
) -> dict:
    if not group_by:
        ts = query_time_series(filters, grain, 120)
        items = [{"time": r["time"], "value": r["value"], "series": "总和"}
                 for r in ts["items"]]
        return {"grain": grain, "rows": len(items), "items": items}

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

    df = repo.load_slice(columns=load_cols, filters=filters, limit=500_000, offset=0)
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
            for time_label, source_column in effective_year_columns.items():
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


# ── Overview ────────────────────────────────────────────────────
def query_overview(
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
def _build_vehicle_frame(filters: dict[str, list[str]]) -> pd.DataFrame:
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

    needed = [c for c in [make_col, model_col, version_col, segment_col, powertrain_col, length_col, msrp_col] if c] + year_cols
    if not needed:
        return pd.DataFrame()

    df = repo.load_slice(columns=needed, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return df

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
    out["Sales"] = _sum_sales_columns(df, year_cols)
    return out


# ── Advanced chart dispatcher ──────────────────────────────────
def query_advanced_chart(
    group: str,
    chart: str,
    filters: dict[str, list[str]],
    top_n: int,
    options: dict | None = None,
) -> dict:
    columns = repo.list_columns()
    ng = str(group).strip().lower()
    nc = str(chart).strip().lower()
    opts = options or {}
    empty = {"group": group, "chart": chart, "rows": 0, "items": []}

    # ── Market structure ────────────────────────────────────────
    if ng == "market_structure":
        if nc == "powertrain_mix":
            col = _resolve_existing_column(POWERTRAIN_CANDIDATES, columns)
            if not col:
                return empty
            items = _aggregate_count(filters, col, top_n)
            return {"group": group, "chart": chart, "rows": len(items), "items": items}

        if nc == "segment_share":
            col = _resolve_existing_column(SEGMENT_CANDIDATES, columns)
            if not col:
                return empty
            items = _aggregate_count(filters, col, top_n)
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
            return _chart_length_vs_price(filters, top_n)

        if nc == "price_per_meter":
            return _chart_price_per_meter(filters, top_n)

        if nc == "sales_vs_price":
            return _chart_sales_vs_price(filters, top_n)

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
            matrix: list[dict] = []
            for row in monthly:
                time_key = str(row.get("time", "")).strip()
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
    vf = _build_vehicle_frame(filters)
    if vf.empty or "Powertrain" not in vf.columns:
        return {"group": "market_structure", "chart": "powertrain_bubble", "rows": 0, "items": []}

    vf = vf[vf["Powertrain"].isin(POWERTRAIN_DISPLAY_ORDER)]
    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[vf["Length"] > 0]
    vf = vf[vf["MSRP"] > 0]

    grain = str(opts.get("grain", "model")).strip().lower()
    use_version_grain = grain == "version" and "Version" in vf.columns

    group_cols = ["Model", "Powertrain"]
    if use_version_grain:
        group_cols.insert(1, "Version")
    if "Brand" in vf.columns:
        group_cols.append("Brand")
    if "Segment" in vf.columns:
        segment_map = vf.groupby(group_cols, dropna=False)["Segment"].agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
    else:
        segment_map = None

    records = []
    for keys, g in vf.groupby(group_cols, dropna=False):
        sales = g["Sales"].fillna(0)
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        if use_version_grain:
            model_name = str(row.get("Model", "")).strip()
            version_name = str(row.get("Version", "")).strip()
            row["DisplayName"] = f"{model_name} / {version_name}".strip(" /")
        else:
            row["DisplayName"] = str(row.get("Model", "")).strip()
        row["Length"] = float(pd.to_numeric(g["Length"], errors="coerce").median())
        row["MSRP"] = float(pd.to_numeric(g["MSRP"], errors="coerce").median())
        row["MsrpMin"] = float(pd.to_numeric(g["MSRP"], errors="coerce").min())
        row["MsrpMax"] = float(pd.to_numeric(g["MSRP"], errors="coerce").max())
        row["VariantCount"] = 1 if use_version_grain else (int(g["Version"].nunique()) if "Version" in g.columns else int(len(g)))
        row["BubbleGrain"] = "version" if use_version_grain else "model"
        if segment_map is not None:
            row["Segment"] = str(segment_map.loc[keys])
        row["Sales"] = float(sales.sum())
        records.append(row)
    agg = pd.DataFrame(records)
    agg = agg.sort_values("Sales", ascending=False).head(max(1, int(top_n)))

    items = agg.to_dict(orient="records")
    return {"group": "market_structure", "chart": "powertrain_bubble", "rows": len(items), "items": items}


# ── Chart: Segment Share by Length ─────────────────────────────
def _chart_segment_share_by_length(
    filters: dict[str, list[str]], opts: dict,
) -> dict:
    vf = _build_vehicle_frame(filters)
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
    load_cols = [c for c in [powertrain_col, range_col, model_col, make_col] if c] + year_cols
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
    df["SalesWindow"] = _sum_sales_columns(df, year_cols)

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

    start_year_label = year_cols[0] if year_cols else None
    end_year_label = year_cols[-1] if year_cols else None
    growth_ready = len(year_cols) >= 2 and start_year_label is not None and end_year_label is not None and start_year_label != end_year_label
    if growth_ready:
        df["SalesWindowStartYear"] = df[start_year_label]
        df["SalesWindowEndYear"] = df[end_year_label]
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
    load_cols = [c for c in [cap_col, msrp_col, powertrain_col, model_col, make_col] if c] + year_cols
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "nev_analysis", "chart": "nev_capacity_vs_msrp", "rows": 0, "items": []}

    df["BatteryCapacity"] = pd.to_numeric(df[cap_col], errors="coerce")
    df["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    df["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    df["Sales"] = _sum_sales_columns(df, year_cols)
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
    if not year_cols:
        return {"group": "price_value", "chart": "price_migration", "rows": 0, "items": []}

    load_cols = [msrp_col] + year_cols
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
    for yc in year_cols:
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
    filters: dict[str, list[str]], top_n: int,
) -> dict:
    vf = _build_vehicle_frame(filters)
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
    filters: dict[str, list[str]], top_n: int,
) -> dict:
    vf = _build_vehicle_frame(filters)
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
    filters: dict[str, list[str]], top_n: int,
) -> dict:
    vf = _build_vehicle_frame(filters)
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
    load_cols = [msrp_col, powertrain_col] + year_cols
    df = repo.load_slice(columns=load_cols, filters=filters, limit=200_000, offset=0)
    if df.empty:
        return {"group": "price_value", "chart": "powertrain_vs_price", "rows": 0, "items": []}

    band_size = int(opts.get("band_size", 1000))
    df["MSRP"] = pd.to_numeric(df[msrp_col], errors="coerce")
    df["Powertrain"] = df[powertrain_col].astype(str).str.strip()
    df["Sales"] = _sum_sales_columns(df, year_cols)
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
    vf = _build_vehicle_frame(filters)
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


def _build_version_frame(filters: dict[str, list[str]]) -> pd.DataFrame:
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

    needed = [c for c in [model_col, version_col, make_col, powertrain_col, length_col, msrp_col, trim_col] if c] + year_cols
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
    out["Sales"] = _sum_sales_columns(df, year_cols)
    return out


def query_model_versions(
    filters: dict[str, list[str]],
    model_name: str,
    top_n: int,
) -> dict:
    """Return version-level scatter for a given Model."""
    vf = _build_version_frame(filters)
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
) -> dict:
    """
    Return positioning scatter with optional KMeans clustering.
    - If target_length/target_msrp provided, filter to nearby vehicles and cluster.
    - manual_competitors: Brand names to force-include.
    - Returns items with cluster_id, and a target_point if target given.
    """
    vf = _build_vehicle_frame(filters)
    if vf.empty or "Length" not in vf.columns or "MSRP" not in vf.columns:
        return {"rows": 0, "items": [], "target": None, "cluster_top3": []}

    vf = vf.dropna(subset=["Length", "MSRP"])
    vf = vf[(vf["Length"] > 0) & (vf["MSRP"] > 0)]

    group_cols = [c for c in ["Brand", "Model", "Segment", "Powertrain"] if c in vf.columns]
    if not group_cols:
        return {"rows": 0, "items": [], "target": None, "cluster_top3": []}

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
