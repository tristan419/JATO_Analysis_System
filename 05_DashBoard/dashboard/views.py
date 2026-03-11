from dataclasses import dataclass
from datetime import date, datetime
from importlib.util import find_spec
import sys
import time
from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .config import (
    APP_TITLE,
    BATTERY_CAPACITY_CANDIDATES,
    BATTERY_RANGE_CANDIDATES,
    COLOR_SEQ,
    CSV_DOWNLOAD_MAX_BYTES,
    CSV_DOWNLOAD_MAX_ROWS,
    LENGTH_CANDIDATES,
    MSRP_CANDIDATES,
    PLOT_CONFIG,
)
from .data import dedupe_preserve_order, get_month_columns, get_year_columns
from .models import ColumnRegistry, FilterSelections
from .styles import style_figure


POWERTRAIN_DISPLAY_ORDER = ["BEV", "MHEV", "PHEV", "ICE", "HEV"]
POWERTRAIN_COLOR_MAP = {
    "BEV": "#22C55E",
    "MHEV": "#F5550B",
    "PHEV": "#3B82F6",
    "ICE": "#9CA3AF",
    "HEV": "#EAB308",
}

EXPORT_LEGEND_POSITIONS = {
    "右侧（默认）": {
        "x": 1.02,
        "y": 1.0,
        "xanchor": "left",
        "yanchor": "top",
        "orientation": "v",
    },
    "顶部": {
        "x": 0.0,
        "y": 1.12,
        "xanchor": "left",
        "yanchor": "bottom",
        "orientation": "h",
    },
    "底部": {
        "x": 0.0,
        "y": -0.18,
        "xanchor": "left",
        "yanchor": "top",
        "orientation": "h",
    },
    "左侧": {
        "x": -0.02,
        "y": 1.0,
        "xanchor": "right",
        "yanchor": "top",
        "orientation": "v",
    },
}

EXPORT_COLOR_SCHEMES: dict[str, list[str] | None] = {
    "保留原图配色": None,
    "Plotly": px.colors.qualitative.Plotly,
    "Safe": px.colors.qualitative.Safe,
    "Set2": px.colors.qualitative.Set2,
    "Pastel": px.colors.qualitative.Pastel,
    "Dark24": px.colors.qualitative.Dark24,
}

EXPORT_AXIS_TICK_STYLE_OPTIONS = [
    "保留原始",
    "整数（千分位）",
    "千分位小数",
    "百分比（0-1）",
    "百分比（原值+%）",
    "科学计数法",
]

EXPORT_DATA_LABEL_MODE_OPTIONS = [
    "关闭",
    "仅数值",
    "仅系列名",
    "仅Model",
    "系列名+数值",
]

EXPORT_DATA_LABEL_POSITION_OPTIONS = [
    "自动",
    "内侧",
    "外侧",
    "顶部",
    "中间",
]

MAX_MANUAL_SERIES_COLOR_CONTROLS = 30

_COMPUTE_CACHE: dict[tuple[object, ...], object] = {}
_TIME_KEY_PARSE_CACHE: dict[tuple[str, str], pd.Timestamp] = {}


def reset_compute_cache() -> None:
    _COMPUTE_CACHE.clear()


@dataclass(frozen=True)
class ChartControls:
    chart_mode: str
    group_label: str | None
    group_column: str | None
    top_n_enabled: bool
    top_n: int
    include_others: bool
    show_line_labels: bool


@dataclass(frozen=True)
class TimeAxis:
    columns: tuple[str, ...]
    labels: tuple[str, ...]
    dates: tuple[pd.Timestamp, ...]
    grain: str


@dataclass(frozen=True)
class TimeSelection:
    columns: tuple[str, ...]
    start_label: str
    end_label: str
    mode: str
    grain: str


def parse_time_key_cached(value: str, grain: str) -> pd.Timestamp:
    normalized_value = str(value).strip()
    cache_key = (grain, normalized_value)
    if cache_key in _TIME_KEY_PARSE_CACHE:
        return _TIME_KEY_PARSE_CACHE[cache_key]

    if grain == "month":
        parsed = pd.to_datetime(
            normalized_value,
            format="%Y %b",
            errors="coerce",
        )
    elif grain == "year":
        parsed = pd.to_datetime(
            normalized_value,
            format="%Y",
            errors="coerce",
        )
    else:
        parsed = pd.to_datetime(normalized_value, errors="coerce")

    _TIME_KEY_PARSE_CACHE[cache_key] = parsed
    return parsed


def build_time_entries(
    columns: list[str],
    grain: str,
) -> list[tuple[pd.Timestamp, str]]:
    entries: list[tuple[pd.Timestamp, str]] = []
    for column in columns:
        label = str(column).strip()
        date_value = parse_time_key_cached(label, grain)
        if pd.notna(date_value):
            entries.append((date_value, label))

    entries.sort(key=lambda item: item[0])
    return entries


def build_time_axis(filtered_df: pd.DataFrame) -> TimeAxis | None:
    month_columns = get_month_columns(filtered_df)
    month_entries = build_time_entries(month_columns, grain="month")

    year_columns = get_year_columns(filtered_df)
    year_entries = build_time_entries(year_columns, grain="year")

    if len(month_entries) >= 2:
        dates = tuple(item[0] for item in month_entries)
        labels = tuple(item[1] for item in month_entries)
        return TimeAxis(
            columns=labels,
            labels=labels,
            dates=dates,
            grain="month",
        )

    if year_entries:
        dates = tuple(item[0] for item in year_entries)
        labels = tuple(item[1] for item in year_entries)
        return TimeAxis(
            columns=labels,
            labels=labels,
            dates=dates,
            grain="year",
        )

    if month_entries:
        dates = tuple(item[0] for item in month_entries)
        labels = tuple(item[1] for item in month_entries)
        return TimeAxis(
            columns=labels,
            labels=labels,
            dates=dates,
            grain="month",
        )

    return None


def format_time_selection(selection: TimeSelection) -> str:
    return f"{selection.start_label} ~ {selection.end_label}"


def show_no_data(scope: str) -> None:
    st.info(f"当前筛选与时间范围下无可展示数据（{scope}）。")


def show_time_axis_unavailable(scope: str) -> None:
    st.warning(f"未识别可用时间轴，无法绘制{scope}。")


def format_euro_value(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"€{float(value):,.0f}"


def summarize_msrp_quality(msrp_values: pd.Series) -> dict[str, float | int]:
    numeric = pd.to_numeric(msrp_values, errors="coerce")
    positive = numeric[numeric > 0]

    if positive.empty:
        return {
            "total_count": int(len(numeric)),
            "non_null_count": int(numeric.notna().sum()),
            "non_positive_count": int((numeric <= 0).sum()),
            "positive_count": 0,
            "p50": float("nan"),
            "p95": float("nan"),
            "high_outlier_count": 0,
        }

    q1 = float(positive.quantile(0.25))
    q3 = float(positive.quantile(0.75))
    iqr = q3 - q1
    high_outlier_threshold = q3 + 1.5 * iqr
    high_outlier_count = int((positive > high_outlier_threshold).sum())

    return {
        "total_count": int(len(numeric)),
        "non_null_count": int(numeric.notna().sum()),
        "non_positive_count": int((numeric <= 0).sum()),
        "positive_count": int(len(positive)),
        "p50": float(positive.quantile(0.50)),
        "p95": float(positive.quantile(0.95)),
        "high_outlier_count": high_outlier_count,
    }


def resolve_slider_indices(
    labels: list[str],
    start_label: str,
    end_label: str,
) -> list[int]:
    if not labels:
        return []

    label_to_index = {
        label: idx
        for idx, label in enumerate(labels)
    }
    start_idx = label_to_index.get(start_label, 0)
    end_idx = label_to_index.get(end_label, len(labels) - 1)
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx

    return list(range(start_idx, end_idx + 1))


def resolve_calendar_indices(
    date_values: list[date],
    start_date: date,
    end_date: date,
) -> list[int]:
    if not date_values:
        return []

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    selected_indices = [
        idx
        for idx, value in enumerate(date_values)
        if start_date <= value <= end_date
    ]
    if selected_indices:
        return selected_indices

    nearest_idx = min(
        range(len(date_values)),
        key=lambda idx: abs((date_values[idx] - start_date).days),
    )
    return [nearest_idx]


def render_time_selector(
    time_axis: TimeAxis,
    key_prefix: str,
    title: str,
) -> TimeSelection:
    st.markdown(f"**{title}**")
    selector_mode = st.radio(
        "时间选择方式",
        ["滑动条拖动", "日历输入"],
        horizontal=True,
        key=f"{key_prefix}_mode",
    )

    labels = list(time_axis.labels)
    columns = list(time_axis.columns)
    date_values = [value.date() for value in time_axis.dates]

    if selector_mode == "滑动条拖动":
        start_label, end_label = st.select_slider(
            "时间范围",
            options=labels,
            value=(labels[0], labels[-1]),
            key=f"{key_prefix}_slider",
        )
        selected_indices = resolve_slider_indices(
            labels,
            start_label,
            end_label,
        )
    else:
        min_date = date_values[0]
        max_date = date_values[-1]
        date_range = st.date_input(
            "时间范围",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key=f"{key_prefix}_calendar",
        )

        if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range

        selected_indices = resolve_calendar_indices(
            date_values,
            start_date,
            end_date,
        )

    selected_columns = tuple(columns[idx] for idx in selected_indices)
    start_label = labels[selected_indices[0]]
    end_label = labels[selected_indices[-1]]

    return TimeSelection(
        columns=selected_columns,
        start_label=start_label,
        end_label=end_label,
        mode=selector_mode,
        grain=time_axis.grain,
    )


def render_global_time_controls(time_axis: TimeAxis) -> TimeSelection:
    with st.container(border=True):
        st.subheader("全局时间轴（默认作用于全部图表）")
        global_selection = render_time_selector(
            time_axis=time_axis,
            key_prefix="global_time",
            title="全局时间范围",
        )
        st.caption(
            f"全局范围：{format_time_selection(global_selection)}"
            f"｜方式：{global_selection.mode}"
        )

    return global_selection


def resolve_chart_time_selection(
    chart_name: str,
    key_prefix: str,
    time_axis: TimeAxis,
    global_selection: TimeSelection,
) -> TimeSelection:
    follow_global_col, info_col = st.columns([1, 3])
    with follow_global_col:
        follow_global = st.checkbox(
            "跟随全局时间轴",
            value=True,
            key=f"{key_prefix}_follow_global",
        )

    if follow_global:
        with info_col:
            st.caption(
                f"{chart_name}：使用全局范围"
                f" {format_time_selection(global_selection)}"
            )
        return global_selection

    with info_col:
        st.caption(f"{chart_name}：使用独立时间轴")
    local_selection = render_time_selector(
        time_axis=time_axis,
        key_prefix=f"{key_prefix}_local_time",
        title=f"{chart_name}时间范围",
    )
    st.caption(
        f"{chart_name}独立范围：{format_time_selection(local_selection)}"
        f"｜方式：{local_selection.mode}"
    )
    return local_selection


def parse_time_keys(series: pd.Series, grain: str) -> pd.Series:
    text_series = series.astype(str).str.strip()
    unique_values = pd.unique(text_series)
    parsed_mapping = {
        value: parse_time_key_cached(value, grain)
        for value in unique_values
    }
    parsed = pd.to_datetime(
        text_series.map(parsed_mapping),
        errors="coerce",
    )

    if parsed.isna().all():
        fallback_mapping = {
            value: parse_time_key_cached(value, "auto")
            for value in unique_values
        }
        parsed = pd.to_datetime(
            text_series.map(fallback_mapping),
            errors="coerce",
        )
    return parsed


def get_numeric_selected_frame(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
) -> pd.DataFrame:
    selected_df = filtered_df[selected_columns]
    non_numeric_columns = [
        column
        for column in selected_columns
        if not pd.api.types.is_numeric_dtype(selected_df[column])
    ]

    if not non_numeric_columns:
        return selected_df

    numeric_df = selected_df.copy()
    for column in non_numeric_columns:
        numeric_df[column] = pd.to_numeric(
            numeric_df[column],
            errors="coerce",
        )
    return numeric_df


def build_time_long_dataframe(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
    grain: str,
    group_column: str | None,
) -> pd.DataFrame:
    if not selected_columns:
        return pd.DataFrame(columns=["Series", "Date", "Sales"])

    numeric_time_df = get_numeric_selected_frame(
        filtered_df,
        selected_columns,
    ).fillna(0.0)

    if group_column:
        series_values = normalize_series(filtered_df[group_column])
        aggregated = numeric_time_df.groupby(
            series_values,
            observed=True,
        ).sum()
        aggregated.index.name = "Series"
        long_df = (
            aggregated.stack(future_stack=True)
            .rename("Sales")
            .reset_index()
        )
        long_df = long_df.rename(columns={"level_1": "TimeKey"})
    else:
        totals = numeric_time_df.sum(axis=0)
        long_df = pd.DataFrame(
            {
                "Series": "总和",
                "TimeKey": totals.index.astype(str),
                "Sales": totals.values,
            }
        )

    long_df["TimeKey"] = long_df["TimeKey"].astype(str)
    long_df["Sales"] = pd.to_numeric(
        long_df["Sales"],
        errors="coerce",
    ).fillna(0.0)
    long_df["Date"] = parse_time_keys(long_df["TimeKey"], grain)
    long_df = long_df.dropna(subset=["Date"]).sort_values("Date")

    return long_df[["Series", "Date", "Sales"]]


def sum_sales_for_columns(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
) -> pd.Series:
    if not selected_columns:
        return pd.Series(0.0, index=filtered_df.index, dtype="float64")

    cache_key = (
        "sum_sales",
        id(filtered_df),
        tuple(selected_columns),
    )
    cached = _COMPUTE_CACHE.get(cache_key)
    if isinstance(cached, pd.Series):
        return cached

    numeric_df = get_numeric_selected_frame(
        filtered_df,
        selected_columns,
    )
    result = numeric_df.fillna(0.0).sum(axis=1)
    _COMPUTE_CACHE[cache_key] = result
    return result


def get_sort_items_callable() -> Callable[..., list[str]] | None:
    try:
        from streamlit_sortables import sort_items as sortable

        return sortable
    except Exception:
        return None


def get_group_dimensions(columns: ColumnRegistry) -> dict[str, str]:
    dimensions: dict[str, str] = {}
    if columns.segment:
        dimensions["细分市场"] = columns.segment
    if columns.powertrain:
        dimensions["动总规整"] = columns.powertrain
    if columns.make:
        dimensions["品牌"] = columns.make
    if columns.model:
        dimensions["Model"] = columns.model
    if columns.version:
        dimensions["Version name"] = columns.version
    return dimensions


def normalize_series(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").fillna("未标注")
    normalized = normalized.str.strip().replace("", "未标注")
    return normalized.astype(str)


def normalize_powertrain_for_nev(series: pd.Series) -> pd.Series:
    normalized = normalize_series(series).str.upper().str.strip()
    compact = normalized.str.replace(r"[\s_\-/]+", "", regex=True)

    normalized = normalized.where(
        ~compact.str.contains("PHEV", na=False),
        "PHEV",
    )
    normalized = normalized.where(
        ~compact.str.contains("BEV", na=False),
        "BEV",
    )
    return normalized.astype(str)


def build_color_map(
    series_values: pd.Series,
    series_order: list[str] | None = None,
) -> dict[str, str]:
    available_values = set(
        series_values.dropna().astype(str).unique().tolist()
    )

    if series_order:
        unique_values = [
            value
            for value in series_order
            if value in available_values
        ]
        remaining_values = sorted(available_values - set(unique_values))
        unique_values.extend(remaining_values)
    else:
        unique_values = sorted(available_values)

    return {
        value: COLOR_SEQ[idx % len(COLOR_SEQ)]
        for idx, value in enumerate(unique_values)
    }


def shorten_label(value: str, max_len: int = 14) -> str:
    text = str(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def add_line_end_labels(fig: Any) -> Any:
    for trace in fig.data:
        x_values = getattr(trace, "x", None)
        if x_values is None:
            continue

        point_count = len(x_values)
        if point_count == 0:
            continue

        label = shorten_label(getattr(trace, "name", ""), max_len=16)
        if point_count == 1:
            text_values = [label]
        else:
            text_values = [""] * (point_count - 1) + [label]

        trace.update(
            mode="lines+markers+text",
            text=text_values,
            textposition="middle right",
            textfont=dict(size=10),
            cliponaxis=False,
        )

    return fig


def export_figure_png(fig: Any) -> bytes:
    return fig.to_image(format="png", scale=2)


def is_kaleido_available() -> bool:
    return find_spec("kaleido") is not None


def get_kaleido_install_command() -> str:
    return f"{sys.executable} -m pip install -r requirements.txt"


def normalize_color_for_picker(
    color: Any,
    fallback: str,
) -> str:
    if isinstance(color, str):
        value = color.strip()
        if value.startswith("#"):
            if len(value) == 4:
                expanded = "#" + "".join(ch * 2 for ch in value[1:])
                try:
                    int(expanded[1:], 16)
                    return expanded.upper()
                except ValueError:
                    pass
            if len(value) == 7:
                try:
                    int(value[1:], 16)
                    return value.upper()
                except ValueError:
                    pass

        lower_value = value.lower()
        if lower_value.startswith("rgb") and "(" in value and ")" in value:
            raw = value[value.find("(") + 1:value.rfind(")")]
            parts = [part.strip() for part in raw.split(",")]
            if len(parts) >= 3:
                try:
                    red = max(0, min(255, int(float(parts[0]))))
                    green = max(0, min(255, int(float(parts[1]))))
                    blue = max(0, min(255, int(float(parts[2]))))
                    return f"#{red:02X}{green:02X}{blue:02X}"
                except ValueError:
                    pass

    return fallback


def get_trace_base_color(trace: Any) -> Any:
    marker = getattr(trace, "marker", None)
    line = getattr(trace, "line", None)

    if marker is not None:
        marker_color = getattr(marker, "color", None)
        if isinstance(marker_color, str):
            return marker_color

    if line is not None:
        line_color = getattr(line, "color", None)
        if isinstance(line_color, str):
            return line_color

    return None


def collect_export_series_color_defaults(fig: Any) -> dict[str, str]:
    series_colors: dict[str, str] = {}

    for index, trace in enumerate(getattr(fig, "data", [])):
        series_name = str(getattr(trace, "name", "")).strip()
        if not series_name or series_name in series_colors:
            continue

        fallback = str(COLOR_SEQ[index % len(COLOR_SEQ)])
        base_color = get_trace_base_color(trace)
        series_colors[series_name] = normalize_color_for_picker(
            base_color,
            normalize_color_for_picker(fallback, "#1F77B4"),
        )

    return series_colors


def build_axis_tick_kwargs(
    style_name: str,
    decimal_places: int,
) -> dict[str, Any]:
    decimals = int(max(0, min(4, decimal_places)))

    if style_name == "整数（千分位）":
        return {"tickformat": ",.0f", "ticksuffix": ""}
    if style_name == "千分位小数":
        return {"tickformat": f",.{decimals}f", "ticksuffix": ""}
    if style_name == "百分比（0-1）":
        return {"tickformat": f".{decimals}%", "ticksuffix": ""}
    if style_name == "百分比（原值+%）":
        return {"tickformat": f",.{decimals}f", "ticksuffix": "%"}
    if style_name == "科学计数法":
        scientific_decimals = max(1, decimals)
        return {"tickformat": f".{scientific_decimals}e", "ticksuffix": ""}

    return {}


def render_global_synced_checkbox(
    label: str,
    global_key: str,
    widget_key: str,
    default: bool = False,
    help_text: str | None = None,
    disabled: bool = False,
) -> bool:
    global_revision_key = f"{global_key}_revision"
    widget_revision_key = f"{widget_key}_revision"

    if global_key not in st.session_state:
        st.session_state[global_key] = bool(default)
    if global_revision_key not in st.session_state:
        st.session_state[global_revision_key] = 0

    global_value = bool(st.session_state[global_key])
    global_revision = int(st.session_state[global_revision_key])

    if widget_key not in st.session_state:
        st.session_state[widget_key] = global_value
        st.session_state[widget_revision_key] = global_revision
    elif int(st.session_state.get(widget_revision_key, -1)) != global_revision:
        st.session_state[widget_key] = global_value
        st.session_state[widget_revision_key] = global_revision

    widget_value = st.checkbox(
        label,
        key=widget_key,
        help=help_text,
        disabled=disabled,
    )

    if bool(widget_value) != global_value:
        st.session_state[global_key] = bool(widget_value)
        st.session_state[global_revision_key] = global_revision + 1
        st.session_state[widget_revision_key] = int(
            st.session_state[global_revision_key]
        )

    return bool(st.session_state[global_key])


def render_global_synced_selectbox(
    label: str,
    options: list[str],
    global_key: str,
    widget_key: str,
    default_option: str,
    help_text: str | None = None,
    disabled: bool = False,
) -> str:
    if not options:
        return ""

    global_revision_key = f"{global_key}_revision"
    widget_revision_key = f"{widget_key}_revision"

    if default_option not in options:
        default_option = options[0]

    if global_key not in st.session_state:
        st.session_state[global_key] = default_option
    if st.session_state[global_key] not in options:
        st.session_state[global_key] = default_option
    if global_revision_key not in st.session_state:
        st.session_state[global_revision_key] = 0

    global_value = str(st.session_state[global_key])
    global_revision = int(st.session_state[global_revision_key])

    if widget_key not in st.session_state:
        st.session_state[widget_key] = global_value
        st.session_state[widget_revision_key] = global_revision
    elif int(st.session_state.get(widget_revision_key, -1)) != global_revision:
        st.session_state[widget_key] = global_value
        st.session_state[widget_revision_key] = global_revision

    selected_value = st.selectbox(
        label,
        options=options,
        key=widget_key,
        help=help_text,
        disabled=disabled,
    )

    if str(selected_value) != global_value:
        st.session_state[global_key] = str(selected_value)
        st.session_state[global_revision_key] = global_revision + 1
        st.session_state[widget_revision_key] = int(
            st.session_state[global_revision_key]
        )

    return str(st.session_state[global_key])


def apply_export_palette(fig: go.Figure, palette: list[str]) -> None:
    if not palette:
        return

    skipped_types = {
        "heatmap",
        "contour",
        "histogram2d",
        "surface",
        "mesh3d",
        "choropleth",
    }
    pie_like_types = {"pie", "sunburst", "treemap", "funnelarea"}

    for index, trace in enumerate(fig.data):
        trace_type = str(getattr(trace, "type", "")).lower()
        if trace_type in skipped_types:
            continue

        if trace_type in pie_like_types:
            if getattr(trace, "marker", None) is None:
                continue
            labels = getattr(trace, "labels", None)
            values = getattr(trace, "values", None)
            segment_count = 0
            if labels is not None:
                segment_count = max(segment_count, len(labels))
            if values is not None:
                segment_count = max(segment_count, len(values))
            if segment_count > 0:
                trace.marker.colors = [
                    palette[i % len(palette)]
                    for i in range(segment_count)
                ]
            continue

        color = palette[index % len(palette)]
        if getattr(trace, "marker", None) is not None:
            trace.marker.color = color
        if getattr(trace, "line", None) is not None:
            trace.line.color = color


def apply_manual_series_colors(
    fig: go.Figure,
    overrides: dict[str, str],
) -> None:
    if not overrides:
        return

    skipped_types = {
        "heatmap",
        "contour",
        "histogram2d",
        "surface",
        "mesh3d",
        "choropleth",
    }

    for trace in fig.data:
        trace_type = str(getattr(trace, "type", "")).lower()
        if trace_type in skipped_types:
            continue

        series_name = str(getattr(trace, "name", "")).strip()
        if not series_name or series_name not in overrides:
            continue

        color = overrides[series_name]
        if getattr(trace, "marker", None) is not None:
            trace.marker.color = color
        if getattr(trace, "line", None) is not None:
            trace.line.color = color


def map_bar_label_position(position_name: str) -> str:
    if position_name == "内侧":
        return "inside"
    if position_name in {"外侧", "顶部"}:
        return "outside"
    if position_name == "中间":
        return "inside"
    return "auto"


def map_scatter_label_position(position_name: str) -> str:
    if position_name == "内侧":
        return "middle center"
    if position_name == "外侧":
        return "top center"
    if position_name == "顶部":
        return "top center"
    if position_name == "中间":
        return "middle center"
    return "top center"


def map_pie_label_position(position_name: str) -> str:
    if position_name == "内侧":
        return "inside"
    if position_name in {"外侧", "顶部"}:
        return "outside"
    if position_name == "中间":
        return "inside"
    return "auto"


def apply_export_data_labels(
    fig: go.Figure,
    mode_name: str,
    position_name: str,
) -> None:
    for trace in fig.data:
        trace_type = str(getattr(trace, "type", "")).lower()

        if trace_type == "bar":
            if mode_name == "关闭":
                trace.text = None
                trace.texttemplate = None
                trace.textposition = "none"
                continue

            orientation = str(getattr(trace, "orientation", "v")).lower()
            value_field = "x" if orientation == "h" else "y"
            series_name = str(getattr(trace, "name", "")).strip()

            if mode_name in {"仅系列名", "仅Model"}:
                trace.texttemplate = series_name or "%{label}"
            elif mode_name == "系列名+数值":
                prefix = f"{series_name}: " if series_name else ""
                trace.texttemplate = f"{prefix}%{{{value_field}}}"
            else:
                trace.texttemplate = f"%{{{value_field}}}"

            trace.textposition = map_bar_label_position(position_name)
            trace.cliponaxis = False
            continue

        if trace_type == "scatter":
            mode_text = str(getattr(trace, "mode", "markers"))
            mode_parts = [part for part in mode_text.split("+") if part]

            if mode_name == "关闭":
                mode_parts = [part for part in mode_parts if part != "text"]
                if not mode_parts:
                    mode_parts = ["markers"]
                trace.mode = "+".join(dict.fromkeys(mode_parts))
                trace.text = None
                trace.texttemplate = None
                continue

            if "text" not in mode_parts:
                mode_parts.append("text")
            if "markers" not in mode_parts and "lines" not in mode_parts:
                mode_parts.insert(0, "markers")
            trace.mode = "+".join(dict.fromkeys(mode_parts))

            series_name = str(getattr(trace, "name", "")).strip()
            value_field = "y" if getattr(trace, "y", None) is not None else "x"
            hover_text = getattr(trace, "hovertext", None)

            model_text: list[str] | None = None
            if hover_text is not None:
                try:
                    hover_items = list(hover_text)
                except TypeError:
                    hover_items = [hover_text]
                normalized_hover_items = [
                    "" if item is None else str(item)
                    for item in hover_items
                ]
                if any(item.strip() for item in normalized_hover_items):
                    model_text = normalized_hover_items

            if mode_name == "仅系列名":
                trace.texttemplate = series_name or "%{text}"
            elif mode_name == "仅Model":
                if model_text:
                    trace.text = model_text
                    trace.texttemplate = "%{text}"
                else:
                    trace.texttemplate = series_name or "%{text}"
            elif mode_name == "系列名+数值":
                prefix = f"{series_name}: " if series_name else ""
                trace.texttemplate = f"{prefix}%{{{value_field}}}"
            else:
                trace.texttemplate = f"%{{{value_field}}}"

            trace.textposition = map_scatter_label_position(position_name)
            continue

        if trace_type in {"heatmap", "contour"}:
            if mode_name == "关闭":
                trace.texttemplate = None
                trace.text = None
                continue

            series_name = str(getattr(trace, "name", "")).strip()
            if mode_name in {"仅系列名", "仅Model"}:
                trace.texttemplate = series_name or "Heatmap"
            elif mode_name == "系列名+数值":
                prefix = f"{series_name}: " if series_name else ""
                trace.texttemplate = f"{prefix}%{{z}}"
            else:
                trace.texttemplate = "%{z}"
            continue

        if trace_type in {"pie", "sunburst", "treemap", "funnelarea"}:
            if mode_name == "关闭":
                trace.textinfo = "none"
                continue

            if mode_name == "仅系列名":
                trace.textinfo = "label"
            elif mode_name == "仅Model":
                trace.textinfo = "label"
            elif mode_name == "系列名+数值":
                trace.textinfo = "label+value"
            else:
                trace.textinfo = "value"

            trace.textposition = map_pie_label_position(position_name)


def apply_export_figure_style(
    fig: Any,
    settings: dict[str, Any],
) -> go.Figure:
    styled_fig = go.Figure(fig)

    show_x_grid = bool(settings["show_x_grid"])
    show_y_grid = bool(settings["show_y_grid"])
    show_axis_line = bool(settings["show_axis_line"])
    show_legend = bool(settings["show_legend"])

    styled_fig.update_layout(
        width=int(settings["width"]),
        height=int(settings["height"]),
        showlegend=show_legend,
        font={"size": int(settings["font_size"])},
        paper_bgcolor=str(settings["paper_bgcolor"]),
        plot_bgcolor=str(settings["plot_bgcolor"]),
    )

    if show_legend:
        legend_position = EXPORT_LEGEND_POSITIONS.get(
            str(settings["legend_position"]),
            EXPORT_LEGEND_POSITIONS["右侧（默认）"],
        )
        styled_fig.update_layout(legend=legend_position)

    title_text = str(settings["title_text"]).strip()
    if title_text:
        styled_fig.update_layout(title={"text": title_text})

    x_title = str(settings["x_title"]).strip()
    if x_title:
        styled_fig.update_xaxes(title_text=x_title)

    y_title = str(settings["y_title"]).strip()
    if y_title:
        styled_fig.update_yaxes(title_text=y_title)

    styled_fig.update_xaxes(
        showgrid=show_x_grid,
        gridcolor=str(settings["grid_color"]),
        showline=show_axis_line,
        linecolor=str(settings["axis_line_color"]),
        mirror=show_axis_line,
    )
    styled_fig.update_yaxes(
        showgrid=show_y_grid,
        gridcolor=str(settings["grid_color"]),
        showline=show_axis_line,
        linecolor=str(settings["axis_line_color"]),
        mirror=show_axis_line,
    )

    x_tick_kwargs = build_axis_tick_kwargs(
        str(settings["x_tick_style"]),
        int(settings["tick_decimal_places"]),
    )
    if x_tick_kwargs:
        styled_fig.update_xaxes(**x_tick_kwargs)

    y_tick_kwargs = build_axis_tick_kwargs(
        str(settings["y_tick_style"]),
        int(settings["tick_decimal_places"]),
    )
    if y_tick_kwargs:
        styled_fig.update_yaxes(**y_tick_kwargs)

    apply_export_data_labels(
        styled_fig,
        mode_name=str(settings["data_label_mode"]),
        position_name=str(settings["data_label_position"]),
    )

    palette_name = str(settings["palette_name"])
    palette = EXPORT_COLOR_SCHEMES.get(palette_name)
    if palette:
        apply_export_palette(styled_fig, palette)

    if bool(settings["manual_series_color_enabled"]):
        apply_manual_series_colors(
            styled_fig,
            dict(settings["series_color_overrides"]),
        )

    return styled_fig


def render_export_style_controls(
    fig: Any,
    chart_key: str,
    kaleido_available: bool,
) -> tuple[dict[str, Any], bool, Any, Any]:
    layout_width = getattr(getattr(fig, "layout", None), "width", None)
    layout_height = getattr(getattr(fig, "layout", None), "height", None)
    default_width = int(layout_width) if layout_width else 1400
    default_height = int(layout_height) if layout_height else 800
    default_width = max(800, min(2400, default_width))
    default_height = max(500, min(1800, default_height))
    series_color_defaults = collect_export_series_color_defaults(fig)

    with st.expander("导出图设置", expanded=False):
        st.caption(
            "支持按导出场景微调图样式（网格线、图例、配色、背景、尺寸等）。"
        )

        grid_col_1, grid_col_2, grid_col_3 = st.columns([1, 1, 1])
        with grid_col_1:
            show_x_grid = st.checkbox(
                "显示X网格线",
                value=True,
                key=f"export_x_grid_{chart_key}",
            )
        with grid_col_2:
            show_y_grid = st.checkbox(
                "显示Y网格线",
                value=True,
                key=f"export_y_grid_{chart_key}",
            )
        with grid_col_3:
            show_axis_line = st.checkbox(
                "显示坐标轴线",
                value=False,
                key=f"export_axis_line_{chart_key}",
            )

        style_col_1, style_col_2, style_col_3 = st.columns([1, 1, 1])
        with style_col_1:
            show_legend = st.checkbox(
                "显示图例",
                value=True,
                key=f"export_show_legend_{chart_key}",
            )
            legend_position = st.selectbox(
                "图例位置",
                options=list(EXPORT_LEGEND_POSITIONS.keys()),
                index=0,
                key=f"export_legend_pos_{chart_key}",
                disabled=not show_legend,
            )
        with style_col_2:
            palette_name = st.selectbox(
                "系列配色",
                options=list(EXPORT_COLOR_SCHEMES.keys()),
                index=0,
                key=f"export_palette_{chart_key}",
            )
            font_size = int(
                st.slider(
                    "字体大小",
                    min_value=8,
                    max_value=24,
                    value=12,
                    step=1,
                    key=f"export_font_size_{chart_key}",
                )
            )
        with style_col_3:
            grid_color = st.color_picker(
                "网格线颜色",
                value="#E5E7EB",
                key=f"export_grid_color_{chart_key}",
            )
            axis_line_color = st.color_picker(
                "坐标轴线颜色",
                value="#6B7280",
                key=f"export_axis_color_{chart_key}",
            )

        axis_col_1, axis_col_2, axis_col_3 = st.columns([1, 1, 1])
        with axis_col_1:
            x_tick_style = st.selectbox(
                "X轴刻度格式",
                options=EXPORT_AXIS_TICK_STYLE_OPTIONS,
                index=0,
                key=f"export_x_tick_style_{chart_key}",
            )
        with axis_col_2:
            y_tick_style = st.selectbox(
                "Y轴刻度格式",
                options=EXPORT_AXIS_TICK_STYLE_OPTIONS,
                index=0,
                key=f"export_y_tick_style_{chart_key}",
            )
        with axis_col_3:
            tick_decimal_places = int(
                st.slider(
                    "小数位",
                    min_value=0,
                    max_value=4,
                    value=1,
                    step=1,
                    key=f"export_tick_decimal_{chart_key}",
                    help="用于小数/百分比/科学计数法。",
                )
            )

        bg_col_1, bg_col_2 = st.columns([1, 1])
        with bg_col_1:
            paper_bgcolor = st.color_picker(
                "整体背景色",
                value="#FFFFFF",
                key=f"export_paper_bg_{chart_key}",
            )
        with bg_col_2:
            plot_bgcolor = st.color_picker(
                "绘图区背景色",
                value="#FFFFFF",
                key=f"export_plot_bg_{chart_key}",
            )

        title_col_1, title_col_2, title_col_3 = st.columns([1, 1, 1])
        with title_col_1:
            title_text = st.text_input(
                "导出标题（可选）",
                value="",
                key=f"export_title_{chart_key}",
                placeholder="留空则沿用图表标题",
            )
        with title_col_2:
            x_title = st.text_input(
                "X轴标题（可选）",
                value="",
                key=f"export_x_title_{chart_key}",
                placeholder="留空则沿用原始标题",
            )
        with title_col_3:
            y_title = st.text_input(
                "Y轴标题（可选）",
                value="",
                key=f"export_y_title_{chart_key}",
                placeholder="留空则沿用原始标题",
            )

        size_col_1, size_col_2 = st.columns([1, 1])
        with size_col_1:
            width = int(
                st.slider(
                    "导出宽度(px)",
                    min_value=800,
                    max_value=2400,
                    value=default_width,
                    step=50,
                    key=f"export_width_{chart_key}",
                )
            )
        with size_col_2:
            height = int(
                st.slider(
                    "导出高度(px)",
                    min_value=500,
                    max_value=1800,
                    value=default_height,
                    step=50,
                    key=f"export_height_{chart_key}",
                )
            )

        label_col_1, label_col_2 = st.columns([1, 1])
        with label_col_1:
            data_label_mode = render_global_synced_selectbox(
                label="数据标签（全局）",
                options=EXPORT_DATA_LABEL_MODE_OPTIONS,
                global_key="export_data_label_mode_global",
                widget_key=f"export_data_label_mode_{chart_key}",
                default_option="关闭",
                help_text="Excel风格：全局联动标签显示模式，影响条形/散点/气泡/热力图/饼图。",
            )
        with label_col_2:
            data_label_position = render_global_synced_selectbox(
                label="标签位置（全局）",
                options=EXPORT_DATA_LABEL_POSITION_OPTIONS,
                global_key="export_data_label_position_global",
                widget_key=f"export_data_label_position_{chart_key}",
                default_option="自动",
                disabled=data_label_mode == "关闭",
                help_text="全局联动标签位置。",
            )

        manual_series_color_enabled = st.checkbox(
            "单系列手工改色",
            value=False,
            key=f"export_manual_series_color_enabled_{chart_key}",
            disabled=not bool(series_color_defaults),
            help="开启后可逐个图例系列指定颜色（导出专用）。",
        )

        series_color_overrides: dict[str, str] = {}
        if manual_series_color_enabled and series_color_defaults:
            series_items = list(series_color_defaults.items())
            if len(series_items) > MAX_MANUAL_SERIES_COLOR_CONTROLS:
                st.caption(
                    "系列过多，仅显示前 "
                    f"{MAX_MANUAL_SERIES_COLOR_CONTROLS} 个进行手工配色。"
                )
                series_items = series_items[:MAX_MANUAL_SERIES_COLOR_CONTROLS]

            st.caption("逐系列颜色设置")
            for index, (series_name, default_color) in enumerate(series_items):
                series_color = st.color_picker(
                    f"{series_name}",
                    value=default_color,
                    key=f"export_series_color_{chart_key}_{index}",
                )
                series_color_overrides[series_name] = series_color

        generate_png = st.button(
            "生成PNG",
            key=f"png_generate_{chart_key}",
            width="content",
            disabled=not kaleido_available,
        )
        export_feedback_slot = st.container()
        export_download_slot = st.container()
        if not kaleido_available:
            install_command = get_kaleido_install_command()
            st.info(
                "PNG 导出依赖 kaleido，当前环境未安装。"
                f"请执行：{install_command}"
            )

    return {
        "show_x_grid": show_x_grid,
        "show_y_grid": show_y_grid,
        "show_axis_line": show_axis_line,
        "show_legend": show_legend,
        "legend_position": legend_position,
        "palette_name": palette_name,
        "font_size": font_size,
        "x_tick_style": x_tick_style,
        "y_tick_style": y_tick_style,
        "tick_decimal_places": tick_decimal_places,
        "grid_color": grid_color,
        "axis_line_color": axis_line_color,
        "paper_bgcolor": paper_bgcolor,
        "plot_bgcolor": plot_bgcolor,
        "title_text": title_text,
        "x_title": x_title,
        "y_title": y_title,
        "width": width,
        "height": height,
        "data_label_mode": data_label_mode,
        "data_label_position": data_label_position,
        "manual_series_color_enabled": manual_series_color_enabled,
        "series_color_overrides": series_color_overrides,
    }, generate_png, export_feedback_slot, export_download_slot


def render_plotly_chart_with_png_export(
    fig: Any,
    chart_key: str,
    filename_prefix: str,
) -> None:
    kaleido_available = is_kaleido_available()
    (
        export_settings,
        generate_png,
        export_feedback_slot,
        export_download_slot,
    ) = render_export_style_controls(
        fig=fig,
        chart_key=chart_key,
        kaleido_available=kaleido_available,
    )
    export_fig = apply_export_figure_style(fig, export_settings)

    st.plotly_chart(export_fig, width="stretch", config=PLOT_CONFIG)

    bytes_key = f"png_bytes_{chart_key}"
    file_key = f"png_file_{chart_key}"
    export_error_message: str | None = None

    if generate_png:
        try:
            png_bytes = export_figure_png(export_fig)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.session_state[bytes_key] = png_bytes
            st.session_state[file_key] = (
                f"{filename_prefix}_{timestamp}.png"
            )
        except Exception as error:
            st.session_state.pop(bytes_key, None)
            st.session_state.pop(file_key, None)
            error_text = f"{type(error).__name__} {error}"
            if "kaleido" in str(error).lower():
                install_command = get_kaleido_install_command()
                export_error_message = (
                    "PNG 导出失败：当前环境缺少 kaleido。"
                    f"请执行：{install_command}"
                )
            else:
                export_error_message = f"PNG 导出失败：{error_text}"

    with export_feedback_slot:
        if export_error_message:
            st.warning(export_error_message)

    png_bytes = st.session_state.get(bytes_key)
    png_name = st.session_state.get(file_key)
    if isinstance(png_bytes, (bytes, bytearray)) and isinstance(
        png_name,
        str,
    ):
        with export_feedback_slot:
            st.success("PNG 已生成，可下载。")
        with export_download_slot:
            st.download_button(
                "下载当前图 PNG",
                data=bytes(png_bytes),
                file_name=png_name,
                mime="image/png",
                key=f"png_download_{chart_key}",
                width="content",
            )


def find_existing_column(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:
    col_map = {str(column).lower().strip(): column for column in df.columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in col_map:
            return col_map[key]
    return None


def to_numeric_flexible(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().all():
        return numeric

    extracted = (
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.extract(r"(-?\d+\.?\d*)")[0]
    )
    extracted_numeric = pd.to_numeric(extracted, errors="coerce")
    return numeric.where(numeric.notna(), extracted_numeric)


def prepare_numeric_axis(
    df: pd.DataFrame,
    candidates: list[str],
) -> tuple[str | None, pd.Series]:
    cache_key = (
        "numeric_axis",
        id(df),
        tuple(str(candidate).strip().lower() for candidate in candidates),
    )
    cached = _COMPUTE_CACHE.get(cache_key)
    if isinstance(cached, tuple) and len(cached) == 2:
        cached_column, cached_series = cached
        if (
            (cached_column is None or isinstance(cached_column, str))
            and isinstance(cached_series, pd.Series)
        ):
            return cached_column, cached_series

    for candidate in candidates:
        column = find_existing_column(df, [candidate])
        if not column:
            continue

        numeric_series = to_numeric_flexible(df[column])
        if numeric_series.notna().sum() > 0:
            result = (column, numeric_series)
            _COMPUTE_CACHE[cache_key] = result
            return result

    empty_result = (None, pd.Series(index=df.index, dtype="float64"))
    _COMPUTE_CACHE[cache_key] = empty_result
    return empty_result


def apply_top_n_series(
    df: pd.DataFrame,
    series_order: list[str] | None,
    include_others: bool,
) -> pd.DataFrame:
    if not series_order or "Series" not in df.columns:
        return df

    if "其他" not in series_order:
        return df

    top_series = {value for value in series_order if value != "其他"}

    if include_others:
        reduced_df = df.copy()
        reduced_df.loc[~reduced_df["Series"].isin(top_series), "Series"] = "其他"
        return reduced_df

    return df[df["Series"].isin(top_series)].copy()


def sort_with_others_last(
    contribution_df: pd.DataFrame,
) -> pd.DataFrame:
    if contribution_df.empty or "Series" not in contribution_df.columns:
        return contribution_df

    regular_df = contribution_df[contribution_df["Series"] != "其他"]
    regular_df = regular_df.sort_values("Sales", ascending=False)
    others_df = contribution_df[contribution_df["Series"] == "其他"]

    if others_df.empty:
        return regular_df

    return pd.concat([regular_df, others_df], ignore_index=True)


def get_series_order(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
) -> list[str]:
    if controls.chart_mode != "分组" or not controls.group_column:
        return ["总和"]

    contribution = get_series_contribution(filtered_df, controls)
    ordered_values = contribution["Series"].astype(str).tolist()

    if controls.top_n_enabled and len(ordered_values) > controls.top_n:
        return ordered_values[:controls.top_n] + ["其他"]

    return ordered_values


def get_series_contribution(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
) -> pd.DataFrame:
    if controls.chart_mode != "分组" or not controls.group_column:
        return pd.DataFrame(columns=["Series", "Sales", "SharePct"])

    cache_key = (
        "series_contribution",
        id(filtered_df),
        controls.group_column,
        controls.chart_mode,
    )
    cached = _COMPUTE_CACHE.get(cache_key)
    if isinstance(cached, pd.DataFrame):
        return cached

    contribution_df = pd.DataFrame()
    contribution_df["Series"] = normalize_series(
        filtered_df[controls.group_column]
    )

    year_columns = get_year_columns(filtered_df)
    month_columns = get_month_columns(filtered_df)
    if year_columns:
        contribution_df["Sales"] = filtered_df[year_columns].sum(axis=1)
    elif month_columns:
        contribution_df["Sales"] = filtered_df[month_columns].sum(axis=1)
    else:
        contribution_df["Sales"] = 1.0

    grouped = (
        contribution_df.groupby("Series", as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
    )

    total_sales = grouped["Sales"].sum()
    if total_sales > 0:
        grouped["SharePct"] = grouped["Sales"] / total_sales
    else:
        grouped["SharePct"] = 0.0

    _COMPUTE_CACHE[cache_key] = grouped
    return grouped


def render_top_n_others_detail(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
) -> None:
    if controls.chart_mode != "分组" or not controls.group_column:
        return
    if not controls.top_n_enabled or "其他" not in series_order:
        return

    contribution = get_series_contribution(filtered_df, controls)
    if contribution.empty:
        return

    top_series = {value for value in series_order if value != "其他"}
    others_df = contribution[~contribution["Series"].isin(top_series)].copy()
    if others_df.empty:
        return

    others_df["占比"] = (others_df["SharePct"] * 100).round(2).astype(str) + "%"
    others_df = others_df.drop(columns=["SharePct"])
    others_df = others_df.rename(
        columns={
            "Series": controls.group_label or "分组",
            "Sales": "销量",
        }
    )

    others_total = float(others_df["销量"].sum())
    overall_total = float(contribution["Sales"].sum())
    others_share = (
        others_total / overall_total * 100
        if overall_total
        else 0.0
    )

    with st.expander("📦 查看“其他”分组明细", expanded=False):
        st.caption(
            f"当前“其他”包含 {len(others_df):,} 个分组，累计销量 {others_total:,.0f}，"
            f"占总量 {others_share:.2f}%"
        )
        st.dataframe(others_df, width="stretch", height=280)


def render_line_mode_controls(
    columns: ColumnRegistry,
) -> ChartControls:
    with st.container(border=True):
        st.subheader("折线显示模式")
        mode_col_1, mode_col_2 = st.columns([2, 1])

        with mode_col_1:
            chart_mode = st.radio(
                "显示方式",
                ["总和", "分组"],
                horizontal=True,
                key="chart_mode_switch",
            )

        with mode_col_2:
            show_line_labels = st.checkbox(
                "显示折线标签",
                value=True,
                key="line_show_labels",
            )

        group_label: str | None = None
        group_column: str | None = None
        top_n_enabled = False
        top_n = 10
        include_others = False
        group_dimensions = get_group_dimensions(columns)

        if chart_mode == "分组":
            if not group_dimensions:
                st.info("缺少可分组字段，已自动切换为总和模式。")
                chart_mode = "总和"
            else:
                labels = list(group_dimensions.keys())
                grouped_col_1, grouped_col_2 = st.columns([2, 1])

                with grouped_col_1:
                    group_label = st.selectbox(
                        "分组维度",
                        labels,
                        key="chart_group_dimension",
                    )

                with grouped_col_2:
                    top_n_enabled = st.checkbox(
                        "启用 Top N",
                        value=True,
                        key="chart_top_n_enabled",
                        help="仅显示销量前 N 的分组，其余可合并为“其他”。",
                    )

                with st.expander("高级设置（分组）", expanded=False):
                    if top_n_enabled:
                        top_n_col_1, top_n_col_2 = st.columns([1, 1])
                        with top_n_col_1:
                            top_n = int(
                                st.number_input(
                                    "Top N 数量",
                                    min_value=3,
                                    max_value=30,
                                    value=10,
                                    step=1,
                                    key="chart_top_n_value",
                                )
                            )

                        with top_n_col_2:
                            include_others = st.checkbox(
                                "图中显示“其他”",
                                value=False,
                                key="chart_include_others",
                                help=(
                                    "关闭后图中隐藏“其他”，"
                                    "但可在明细展开查看。"
                                ),
                            )
                    else:
                        top_n = 10
                        include_others = False
                        st.caption(
                            "已关闭 Top N：展示全部分组，不合并“其他”。"
                        )

                group_column = group_dimensions[group_label]
        else:
            st.caption(
                "切换到“分组”后，可按细分市场/动总规整/品牌/Model/Version name 分色显示。"
            )

    return ChartControls(
        chart_mode=chart_mode,
        group_label=group_label,
        group_column=group_column,
        top_n_enabled=top_n_enabled,
        top_n=top_n,
        include_others=include_others,
        show_line_labels=show_line_labels,
    )


def render_header_card(filtered_df: pd.DataFrame) -> None:
    with st.container(border=True):
        title_col, metric_col = st.columns([4, 1])
        with title_col:
            st.markdown(f"### 🚗 {APP_TITLE}")
        with metric_col:
            st.metric("筛选后记录数", f"{len(filtered_df):,}")


def render_kpi_cards(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    year_columns = get_year_columns(filtered_df)
    full_cycle_year_sales = (
        filtered_df[year_columns].sum().sum() if year_columns else 0
    )

    selected_columns: list[str] = []
    selection_scope = "全周期年度列"
    if time_axis and global_time_selection and global_time_selection.columns:
        selected_columns = list(global_time_selection.columns)
        selection_scope = (
            f"全局时间窗 {global_time_selection.start_label}"
            f" ~ {global_time_selection.end_label}"
        )
    elif year_columns:
        selected_columns = year_columns

    scoped_sales = float(
        sum_sales_for_columns(filtered_df, selected_columns).sum()
    )

    brand_count = (
        filtered_df[columns.make].nunique(dropna=True) if columns.make else 0
    )
    model_count = (
        filtered_df[columns.model].nunique(dropna=True) if columns.model else 0
    )
    version_count = (
        filtered_df[columns.version].nunique(dropna=True)
        if columns.version
        else 0
    )

    kpi_col_1, kpi_col_2, kpi_col_3, kpi_col_4 = st.columns(4)
    with kpi_col_1:
        st.metric("累计销量（全局时间窗）", f"{scoped_sales:,.0f}")
    with kpi_col_2:
        st.metric("品牌数", f"{brand_count:,}")
    with kpi_col_3:
        st.metric("Model 数", f"{model_count:,}")
    with kpi_col_4:
        st.metric("Version 数", f"{version_count:,}")

    st.caption(
        f"KPI 口径：{selection_scope}｜"
        f"全周期年度列合计：{full_cycle_year_sales:,.0f}"
    )


def render_year_tab(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> dict[str, float]:
    stats = {
        "Year Transform": 0.0,
        "Year Plot": 0.0,
    }

    with st.container(border=True):
        st.subheader("年度对比")

        if not time_axis or not global_time_selection:
            show_time_axis_unavailable("年度趋势")
            return stats

        time_selection = resolve_chart_time_selection(
            chart_name="年度趋势",
            key_prefix="year_chart",
            time_axis=time_axis,
            global_selection=global_time_selection,
        )
        selected_columns = list(time_selection.columns)
        if not selected_columns:
            show_no_data("年度趋势")
            return stats

        transform_start = time.perf_counter()

        group_column = (
            controls.group_column
            if controls.chart_mode == "分组"
            else None
        )
        yearly_long = build_time_long_dataframe(
            filtered_df=filtered_df,
            selected_columns=selected_columns,
            grain=time_selection.grain,
            group_column=group_column,
        )
        if yearly_long.empty:
            show_no_data("年度趋势")
            return stats

        yearly_long["YearNum"] = yearly_long["Date"].dt.year
        yearly_long = apply_top_n_series(
            yearly_long,
            series_order=series_order,
            include_others=controls.include_others,
        )
        year_plot = yearly_long.groupby(
            ["Series", "YearNum"],
            as_index=False,
        )["Sales"].sum()
        year_plot = year_plot.sort_values(["YearNum", "Series"])
        year_plot["Year"] = year_plot["YearNum"].astype("Int64").astype(str)
        stats["Year Transform"] = time.perf_counter() - transform_start

        plot_start = time.perf_counter()
        color_map = build_color_map(
            year_plot["Series"],
            series_order=series_order,
        )
        title = (
            f"年度趋势（按{controls.group_label}）"
            if controls.chart_mode == "分组" and controls.group_label
            else "年度趋势（总和）"
        )

        fig = px.line(
            year_plot,
            x="Year",
            y="Sales",
            color="Series",
            markers=True,
            title=title,
            color_discrete_map=color_map,
        )

        if controls.show_line_labels:
            fig = add_line_end_labels(fig)

        st.plotly_chart(
            style_figure(fig),
            width="stretch",
            config=PLOT_CONFIG,
        )
        if controls.show_line_labels:
            st.caption("折线标签=分组名称；点位=该年销量。")
        else:
            st.caption("点位=该年销量。")

        stats["Year Plot"] = time.perf_counter() - plot_start
        return stats


def convert_dates_to_period_start(
    date_series: pd.Series,
    axis_level: str,
) -> pd.Series:
    cache_key = (
        "period_start",
        id(date_series),
        axis_level,
    )
    cached = _COMPUTE_CACHE.get(cache_key)
    if isinstance(cached, pd.Series):
        return cached

    parsed_dates = pd.to_datetime(date_series, errors="coerce")
    if axis_level == "月":
        values = (
            parsed_dates.to_numpy(dtype="datetime64[ns]")
            .astype("datetime64[M]")
            .astype("datetime64[ns]")
        )
        result = pd.Series(values, index=parsed_dates.index)
    elif axis_level == "年":
        values = (
            parsed_dates.to_numpy(dtype="datetime64[ns]")
            .astype("datetime64[Y]")
            .astype("datetime64[ns]")
        )
        result = pd.Series(values, index=parsed_dates.index)
    else:
        result = pd.to_datetime(
            {
                "year": parsed_dates.dt.year,
                "month": (parsed_dates.dt.quarter - 1) * 3 + 1,
                "day": 1,
            },
            errors="coerce",
        )

    _COMPUTE_CACHE[cache_key] = result
    return result


def render_month_tab(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> dict[str, float]:
    stats = {
        "Month Transform": 0.0,
        "Month Plot": 0.0,
    }

    with st.container(border=True):
        st.subheader("月度细化（支持时间轴调整）")

        if not time_axis or not global_time_selection:
            show_time_axis_unavailable("月度细化")
            return stats

        time_selection = resolve_chart_time_selection(
            chart_name="月度细化",
            key_prefix="month_chart",
            time_axis=time_axis,
            global_selection=global_time_selection,
        )
        selected_columns = list(time_selection.columns)
        if not selected_columns:
            show_no_data("月度细化")
            return stats

        transform_start = time.perf_counter()

        group_column = (
            controls.group_column
            if controls.chart_mode == "分组"
            else None
        )
        period_df = build_time_long_dataframe(
            filtered_df=filtered_df,
            selected_columns=selected_columns,
            grain=time_selection.grain,
            group_column=group_column,
        )
        if period_df.empty:
            show_no_data("月度细化")
            return stats

        period_df = apply_top_n_series(
            period_df,
            series_order=series_order,
            include_others=controls.include_others,
        )
        st.metric("当前图表时间窗销量总和", f"{period_df['Sales'].sum():,.0f}")

        axis_options = ["月", "季度", "年"]
        if time_selection.grain != "month":
            axis_options = ["年"]
            st.caption("当前仅存在年度时间轴，已切换为年度粒度展示。")
        axis_level = st.selectbox(
            "时间轴粒度",
            axis_options,
            index=0,
            key="month_axis_level",
        )

        period_df["Period"] = convert_dates_to_period_start(
            period_df["Date"],
            axis_level,
        )

        grouped = period_df.groupby(
            ["Series", "Period"],
            as_index=False,
        )["Sales"].sum()
        if grouped.empty:
            show_no_data(f"{axis_level}度趋势")
            return stats

        grouped = grouped.sort_values(["Period", "Series"])
        stats["Month Transform"] = time.perf_counter() - transform_start

        plot_start = time.perf_counter()
        color_map = build_color_map(
            grouped["Series"],
            series_order=series_order,
        )
        title = (
            f"{axis_level}度销量趋势（按{controls.group_label}）"
            if controls.chart_mode == "分组" and controls.group_label
            else f"{axis_level}度销量趋势（总和）"
        )
        fig = px.line(
            grouped,
            x="Period",
            y="Sales",
            color="Series",
            markers=True,
            title=title,
            color_discrete_map=color_map,
        )
        if controls.show_line_labels:
            fig = add_line_end_labels(fig)
        st.plotly_chart(
            style_figure(fig),
            width="stretch",
            config=PLOT_CONFIG,
        )
        if controls.show_line_labels:
            st.caption("折线标签=分组名称；点位=该时间粒度销量。")
        else:
            st.caption("点位=该时间粒度销量。")

        stats["Month Plot"] = time.perf_counter() - plot_start
        return stats


def get_time_selection_for_chart(
    chart_name: str,
    key_prefix: str,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> tuple[TimeSelection | None, list[str]]:
    if not time_axis or not global_time_selection:
        show_time_axis_unavailable(chart_name)
        return None, []

    selection = resolve_chart_time_selection(
        chart_name=chart_name,
        key_prefix=key_prefix,
        time_axis=time_axis,
        global_selection=global_time_selection,
    )
    selected_columns = list(selection.columns)
    if not selected_columns:
        show_no_data(chart_name)
        return None, []

    return selection, selected_columns


def group_selected_columns_by_year(
    selected_columns: list[str],
    grain: str,
) -> dict[str, list[str]]:
    year_map: dict[str, list[str]] = {}
    if grain == "year":
        for column in selected_columns:
            year_map[str(column)] = [column]
    else:
        for column in selected_columns:
            parsed = pd.to_datetime(
                str(column),
                format="%Y %b",
                errors="coerce",
            )
            if pd.isna(parsed):
                parsed = pd.to_datetime(str(column), errors="coerce")
            if pd.isna(parsed):
                continue
            year_key = str(parsed.year)
            year_map.setdefault(year_key, []).append(column)

    sorted_keys = sorted(
        year_map.keys(),
        key=lambda value: int(value) if value.isdigit() else value,
    )
    return {key: year_map[key] for key in sorted_keys}


def make_price_bands(
    values: pd.Series,
    band_size: int,
) -> tuple[pd.Series, list[str]]:
    if band_size <= 0:
        band_size = 100

    numeric = pd.to_numeric(values, errors="coerce")
    numeric = numeric.where(numeric > 0)
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(index=values.index, dtype="string"), []

    lower = int((valid.min() // band_size) * band_size)
    upper = int(((valid.max() // band_size) + 1) * band_size)
    edges = list(range(lower, upper + band_size, band_size))
    if len(edges) < 2:
        edges = [lower, lower + band_size]

    labels = [
        f"€{int(start):,}-€{int(start + band_size):,}"
        for start in edges[:-1]
    ]
    bands = pd.cut(
        numeric,
        bins=edges,
        labels=labels,
        right=False,
        include_lowest=True,
    )
    return bands.astype("string"), labels


def make_length_bands(
    values: pd.Series,
    band_size: int,
) -> tuple[pd.Series, list[str]]:
    numeric = pd.to_numeric(values, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(index=values.index, dtype="string"), []

    lower = int((valid.min() // band_size) * band_size)
    upper = int(((valid.max() // band_size) + 1) * band_size)
    edges = list(range(lower, upper + band_size, band_size))
    if len(edges) < 2:
        edges = [lower, lower + band_size]

    labels = [
        f"{int(start)}-{int(start + band_size)}mm"
        for start in edges[:-1]
    ]
    bands = pd.cut(
        numeric,
        bins=edges,
        labels=labels,
        right=False,
        include_lowest=True,
    )
    return bands.astype("string"), labels


def build_price_frame(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selected_columns: list[str],
) -> tuple[pd.DataFrame, str | None]:
    msrp_col, msrp_values = prepare_numeric_axis(
        filtered_df,
        list(MSRP_CANDIDATES),
    )
    if not msrp_col:
        return pd.DataFrame(), None

    sales_ref = sum_sales_for_columns(filtered_df, selected_columns)
    if columns.model:
        model_series = normalize_series(filtered_df[columns.model])
    else:
        model_series = pd.Series("未标注", index=filtered_df.index)

    if columns.make:
        brand_series = normalize_series(filtered_df[columns.make])
    else:
        brand_series = pd.Series("全部品牌", index=filtered_df.index)

    if columns.segment:
        segment_series = normalize_series(filtered_df[columns.segment])
    else:
        segment_series = pd.Series("未标注", index=filtered_df.index)

    if columns.powertrain:
        powertrain_series = normalize_series(
            filtered_df[columns.powertrain]
        )
    else:
        powertrain_series = pd.Series("未标注", index=filtered_df.index)

    frame = pd.DataFrame(
        {
            "Model": model_series,
            "Brand": brand_series,
            "Segment": segment_series,
            "Powertrain": powertrain_series,
            "MSRP": msrp_values,
            "Sales": sales_ref,
        }
    )
    frame = frame.dropna(subset=["MSRP", "Sales"])
    frame["Sales"] = pd.to_numeric(
        frame["Sales"],
        errors="coerce",
    ).fillna(0.0)
    return frame, msrp_col


def build_vehicle_frame(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selected_columns: list[str],
) -> tuple[pd.DataFrame, str | None, str | None]:
    price_frame, msrp_col = build_price_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if price_frame.empty:
        return pd.DataFrame(), None, msrp_col

    length_col, length_values = prepare_numeric_axis(
        filtered_df,
        list(LENGTH_CANDIDATES),
    )
    if not length_col:
        return pd.DataFrame(), None, msrp_col

    price_frame = price_frame.copy()
    price_frame["Length"] = length_values
    price_frame = price_frame.dropna(subset=["Length"])
    return price_frame, length_col, msrp_col


def render_chart_price_migration(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Price Migration",
        key_prefix="adv_price_migration",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    msrp_col, msrp_values = prepare_numeric_axis(
        filtered_df,
        list(MSRP_CANDIDATES),
    )
    if not msrp_col:
        st.warning("缺少 MSRP 数值列，无法绘制价格带迁移图。")
        return

    year_map = group_selected_columns_by_year(
        selected_columns,
        selection.grain,
    )
    if not year_map:
        st.info("当前时间范围无法形成年度分组。")
        return

    control_col_1, control_col_2, control_col_3 = st.columns([1, 1, 1])
    with control_col_1:
        band_size = int(
            st.slider(
                "价格带宽（€）",
                min_value=100,
                max_value=3000,
                value=1000,
                step=100,
                key="adv_price_migration_band_size",
            )
        )
    with control_col_2:
        chart_type = st.radio(
            "图形类型",
            ["折线图", "堆叠面积图"],
            horizontal=True,
            key="adv_price_migration_type",
        )
    with control_col_3:
        split_by_powertrain = st.checkbox(
            "分动总查看",
            value=False,
            key="adv_price_migration_split_powertrain",
            disabled=not bool(columns.powertrain),
            help="开启后按动总拆分子图，对比各动总内部的价格带迁移。",
        )

    work_df = pd.DataFrame(index=filtered_df.index)
    work_df["MSRP"] = msrp_values
    for year_key, year_columns in year_map.items():
        work_df[f"Sales_{year_key}"] = sum_sales_for_columns(
            filtered_df,
            year_columns,
        )

    if split_by_powertrain and columns.powertrain:
        work_df["Powertrain"] = normalize_series(
            filtered_df[columns.powertrain]
        )

    price_quality = summarize_msrp_quality(work_df["MSRP"])
    invalid_msrp_count = int(price_quality["non_positive_count"])

    work_df["PriceBand"], band_order = make_price_bands(
        work_df["MSRP"],
        band_size,
    )
    work_df = work_df.dropna(subset=["PriceBand"])
    if work_df.empty:
        show_no_data("价格带迁移")
        return

    selected_powertrains: list[str] | None = None
    if split_by_powertrain and "Powertrain" in work_df.columns:
        unique_powertrains = set(work_df["Powertrain"].astype(str))
        ordered_powertrains = [
            value
            for value in POWERTRAIN_DISPLAY_ORDER
            if value in unique_powertrains
        ]
        remaining_powertrains = sorted(
            unique_powertrains - set(ordered_powertrains)
        )
        powertrain_options = ordered_powertrains + remaining_powertrains
        default_powertrains = (
            ordered_powertrains
            if ordered_powertrains
            else powertrain_options
        )
        with st.expander("高级设置", expanded=False):
            selected_powertrains = st.multiselect(
                "动总类型",
                options=powertrain_options,
                default=default_powertrains,
                key="adv_price_migration_powertrain_options",
            )
        if not selected_powertrains:
            st.info("请至少选择一个动总类型。")
            return
        work_df = work_df[
            work_df["Powertrain"].isin(selected_powertrains)
        ]
        if work_df.empty:
            st.info("所选动总类型在当前筛选下无可用价格带数据。")
            return

    if invalid_msrp_count > 0:
        st.caption(
            f"已自动排除 MSRP≤0 的记录 {invalid_msrp_count:,} 条。"
        )
    if int(price_quality["positive_count"]) > 0:
        st.caption(
            "MSRP质量："
            f"有效 {int(price_quality['positive_count']):,} 条｜"
            f"P50={format_euro_value(float(price_quality['p50']))}｜"
            f"P95={format_euro_value(float(price_quality['p95']))}｜"
            f"IQR高价异常 {int(price_quality['high_outlier_count']):,} 条"
        )

    value_columns = [f"Sales_{year_key}" for year_key in year_map.keys()]
    id_vars = ["PriceBand"]
    if split_by_powertrain and "Powertrain" in work_df.columns:
        id_vars.append("Powertrain")

    migration_long = work_df.melt(
        id_vars=id_vars,
        value_vars=value_columns,
        var_name="YearKey",
        value_name="Sales",
    )
    migration_long["Year"] = migration_long["YearKey"].str.replace(
        "Sales_",
        "",
        regex=False,
    )
    group_fields = ["PriceBand", "Year"]
    if split_by_powertrain and "Powertrain" in migration_long.columns:
        group_fields = ["Powertrain", "PriceBand", "Year"]

    migration_plot = migration_long.groupby(
        group_fields,
        as_index=False,
    )["Sales"].sum()
    migration_plot["PriceBand"] = pd.Categorical(
        migration_plot["PriceBand"],
        categories=band_order,
        ordered=True,
    )
    sort_fields = ["PriceBand", "Year"]
    if split_by_powertrain and "Powertrain" in migration_plot.columns:
        sort_fields = ["Powertrain", "PriceBand", "Year"]
    migration_plot = migration_plot.sort_values(sort_fields)

    color_map = build_color_map(migration_plot["Year"])
    if split_by_powertrain and "Powertrain" in migration_plot.columns:
        plot_kwargs = {
            "facet_col": "Powertrain",
            "facet_col_wrap": 3,
            "category_orders": {
                "Powertrain": selected_powertrains or [],
            },
        }
        if chart_type == "堆叠面积图":
            fig = px.area(
                migration_plot,
                x="PriceBand",
                y="Sales",
                color="Year",
                title="Price Migration（按动总）",
                color_discrete_map=color_map,
                **plot_kwargs,
            )
        else:
            fig = px.line(
                migration_plot,
                x="PriceBand",
                y="Sales",
                color="Year",
                markers=True,
                title="Price Migration（按动总）",
                color_discrete_map=color_map,
                **plot_kwargs,
            )
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace("Powertrain=", "")
            )
        )
    else:
        if chart_type == "堆叠面积图":
            fig = px.area(
                migration_plot,
                x="PriceBand",
                y="Sales",
                color="Year",
                title="Price Migration（价格带迁移图）",
                color_discrete_map=color_map,
            )
        else:
            fig = px.line(
                migration_plot,
                x="PriceBand",
                y="Sales",
                color="Year",
                markers=True,
                title="Price Migration（价格带迁移图）",
                color_discrete_map=color_map,
            )

    fig = style_figure(fig)
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_price_migration",
        filename_prefix="price_migration",
    )

    if split_by_powertrain and "Powertrain" in migration_plot.columns:
        peak_rows = migration_plot.loc[
            migration_plot.groupby(["Powertrain", "Year"])["Sales"].idxmax()
        ][["Powertrain", "Year", "PriceBand"]]
        peak_rows = peak_rows.sort_values(["Powertrain", "Year"])
        st.caption("峰值价格带观察（按动总）")
        st.dataframe(
            peak_rows.rename(
                columns={
                    "Powertrain": "动总",
                    "Year": "年份",
                    "PriceBand": "峰值价格带",
                }
            ),
            width="stretch",
            hide_index=True,
        )
    else:
        peak_rows = migration_plot.loc[
            migration_plot.groupby("Year")["Sales"].idxmax()
        ][["Year", "PriceBand"]]
        peak_rows = peak_rows.sort_values("Year")
        peak_text = "；".join(
            f"{row['Year']} 峰值={row['PriceBand']}"
            for _, row in peak_rows.iterrows()
        )
        st.caption(f"峰值价格带观察：{peak_text}")

    st.caption(f"MSRP 列：{msrp_col}；价格带宽：€{band_size:,}")


def render_chart_length_vs_price_map(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Length vs Price",
        key_prefix="adv_length_price",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    vehicle_df, length_col, msrp_col = build_vehicle_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if vehicle_df.empty or not length_col or not msrp_col:
        st.warning("缺少车长或 MSRP 数值列，无法绘制尺寸—价格地图。")
        return

    aggregate_df = vehicle_df.groupby(
        ["Brand", "Model", "Segment", "Powertrain"],
        as_index=False,
    ).agg(
        Length=("Length", "median"),
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    )
    aggregate_df = aggregate_df.dropna(subset=["Length", "MSRP", "Sales"])
    if aggregate_df.empty:
        show_no_data("尺寸—价格地图")
        return

    top_n = 180
    with st.expander("高级设置", expanded=False):
        top_n = int(
            st.slider(
                "展示车型数（按销量）",
                min_value=30,
                max_value=500,
                value=top_n,
                step=10,
                key="adv_length_price_topn",
            )
        )
    aggregate_df = aggregate_df.sort_values(
        "Sales",
        ascending=False,
    ).head(top_n)

    fig = px.scatter(
        aggregate_df,
        x="Length",
        y="MSRP",
        color="Segment",
        size="Sales",
        hover_name="Model",
        hover_data={
            "Brand": True,
            "Powertrain": True,
            "Sales": ":,.0f",
            "Length": ":,.0f",
            "MSRP": ":,.0f",
        },
        title="Length vs Price（尺寸—价格地图）",
        size_max=42,
    )
    fig = style_figure(fig)
    fig.update_xaxes(title=f"车长（{length_col}）")
    fig.update_yaxes(title=f"MSRP（{msrp_col}）")
    fig.add_vline(x=4550, line_dash="dash", line_color="#94A3B8")
    fig.add_vline(x=4700, line_dash="dash", line_color="#64748B")

    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_length_price_map",
        filename_prefix="length_price_map",
    )

    c_segment = aggregate_df[
        (aggregate_df["Length"] >= 4400)
        & (aggregate_df["Length"] <= 4550)
    ]
    if not c_segment.empty:
        c_price_upper = c_segment["MSRP"].quantile(0.75)
        value_gap = aggregate_df[
            (aggregate_df["Length"] >= 4700)
            & (aggregate_df["MSRP"] <= c_price_upper)
        ]
        st.caption(
            (
                "C-SUV 聚集区约 4400–4550mm，D-SUV 参考线约 4700mm；"
                f"检测到潜在“越级价值”车型 {len(value_gap):,} 款。"
            )
        )


def render_chart_price_per_meter_vs_sales(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Price per Meter vs Sales",
        key_prefix="adv_price_per_meter",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    vehicle_df, length_col, msrp_col = build_vehicle_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if vehicle_df.empty or not length_col or not msrp_col:
        st.warning("缺少车长或 MSRP 数值列，无法绘制单位尺寸价格图。")
        return

    vehicle_df = vehicle_df.copy()
    vehicle_df["LengthMeter"] = vehicle_df["Length"] / 1000.0
    vehicle_df["PricePerMeter"] = (
        vehicle_df["MSRP"] / vehicle_df["LengthMeter"]
    )
    vehicle_df = vehicle_df.dropna(subset=["PricePerMeter", "Sales"])

    model_df = vehicle_df.groupby(
        ["Brand", "Model"],
        as_index=False,
    ).agg(
        PricePerMeter=("PricePerMeter", "median"),
        Sales=("Sales", "sum"),
    )
    model_df = model_df.sort_values("Sales", ascending=False)
    if model_df.empty:
        show_no_data("单位尺寸价格 vs 销量")
        return

    top_n = 50
    with st.expander("高级设置", expanded=False):
        top_n = int(
            st.slider(
                "展示车型数（按销量）",
                min_value=10,
                max_value=200,
                value=top_n,
                step=5,
                key="adv_price_per_meter_topn",
            )
        )
    model_df = model_df.head(top_n)

    fig = px.scatter(
        model_df,
        x="PricePerMeter",
        y="Sales",
        color="Brand",
        hover_name="Model",
        hover_data={
            "PricePerMeter": ":,.0f",
            "Sales": ":,.0f",
        },
        title="Price per Meter vs Sales（单位尺寸价格 vs 销量）",
    )
    fig = style_figure(fig)
    fig.update_xaxes(title="单位尺寸价格（€/m）")
    fig.update_yaxes(title="销量")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_price_per_meter_sales",
        filename_prefix="price_per_meter_sales",
    )

    st.caption(
        f"价格密度 = MSRP / 车长（米）；车长列：{length_col}，MSRP 列：{msrp_col}。"
    )


def render_chart_powertrain_vs_price(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Powertrain vs Price",
        key_prefix="adv_powertrain_price",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    if not columns.powertrain:
        st.warning("缺少动总字段，无法绘制动力结构 vs 价格图。")
        return

    price_frame, msrp_col = build_price_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if price_frame.empty or not msrp_col:
        st.warning("缺少 MSRP 数据，无法绘制动力结构 vs 价格图。")
        return

    split_dimensions: dict[str, str] = {}
    if columns.make:
        split_dimensions["品牌"] = "Brand"
    if columns.country:
        split_dimensions["国家"] = "Country"

    control_col_1, control_col_2 = st.columns([1, 1])
    with control_col_1:
        band_size = int(
            st.slider(
                "价格带宽（€）",
                min_value=100,
                max_value=3000,
                value=1000,
                step=100,
                key="adv_powertrain_price_band_size",
            )
        )

    with control_col_2:
        split_enabled = st.checkbox(
            "拆分查看",
            value=False,
            key="adv_powertrain_price_split_enabled",
            disabled=not bool(split_dimensions),
            help="按维度分面查看各价格段动总结构。",
        )

    split_label: str | None = None
    split_column: str | None = None
    price_frame = price_frame.copy()
    if columns.country:
        price_frame["Country"] = normalize_series(
            filtered_df.loc[price_frame.index, columns.country]
        )

    price_frame["PriceBand"], band_order = make_price_bands(
        price_frame["MSRP"],
        band_size,
    )
    price_frame = price_frame.dropna(subset=["PriceBand"])
    if price_frame.empty:
        show_no_data("动力结构 vs 价格")
        return

    group_count = 6
    if split_enabled and split_dimensions:
        with st.expander("高级设置", expanded=False):
            split_label = st.selectbox(
                "拆分维度",
                options=list(split_dimensions.keys()),
                key="adv_powertrain_price_split_dimension",
            )
            split_column = split_dimensions.get(split_label)

            preview_totals = (
                price_frame.groupby(split_column, as_index=False)["Sales"]
                .sum()
                .sort_values("Sales", ascending=False)
                if split_column
                else pd.DataFrame()
            )
            if preview_totals.empty:
                st.info("当前筛选下无可拆分数据。")
                return

            max_groups = min(12, len(preview_totals))
            group_count = int(
                st.slider(
                    "最多拆分组数",
                    min_value=1,
                    max_value=max_groups,
                    value=min(6, max_groups),
                    step=1,
                    key="adv_powertrain_price_split_group_count",
                )
            )

    selected_groups: list[str] | None = None
    if split_enabled and split_column:
        split_totals = (
            price_frame.groupby(split_column, as_index=False)["Sales"]
            .sum()
            .sort_values("Sales", ascending=False)
        )
        if split_totals.empty:
            st.info("当前筛选下无可拆分数据。")
            return
        selected_groups = (
            split_totals.head(group_count)[split_column]
            .astype(str)
            .tolist()
        )
        price_frame = price_frame[
            price_frame[split_column].astype(str).isin(selected_groups)
        ]
        if price_frame.empty:
            st.info("所选拆分组在当前筛选下无可用价格带数据。")
            return

    group_fields = ["PriceBand", "Powertrain"]
    if split_enabled and split_column:
        group_fields = [split_column, "PriceBand", "Powertrain"]

    pt_df = price_frame.groupby(group_fields, as_index=False)["Sales"].sum()
    pt_df["PriceBand"] = pd.Categorical(
        pt_df["PriceBand"],
        categories=band_order,
        ordered=True,
    )
    sort_fields = ["PriceBand"]
    if split_enabled and split_column:
        sort_fields = [split_column, "PriceBand"]
    pt_df = pt_df.sort_values(sort_fields)

    category_orders: dict[str, list[str]] = {
        "Powertrain": POWERTRAIN_DISPLAY_ORDER,
    }
    facet_kwargs: dict[str, str | int] = {}
    title = "Powertrain vs Price（动力结构 vs 价格）"
    if split_enabled and split_column:
        title = f"Powertrain vs Price（按{split_label}）"
        category_orders[split_column] = selected_groups or []
        facet_kwargs = {
            "facet_col": split_column,
            "facet_col_wrap": 3,
        }

    fig = px.bar(
        pt_df,
        x="PriceBand",
        y="Sales",
        color="Powertrain",
        title=title,
        category_orders=category_orders,
        color_discrete_map=POWERTRAIN_COLOR_MAP,
        **facet_kwargs,
    )
    fig.update_layout(barmode="stack")
    fig.update_traces(
        texttemplate="%{percentParent:.0%}",
        textposition="inside",
    )
    if split_enabled and split_column:
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace(f"{split_column}=", "")
            )
        )
    fig = style_figure(fig)
    fig.update_xaxes(title="价格带")
    fig.update_yaxes(title="销量")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_powertrain_price_mix",
        filename_prefix="powertrain_price_mix",
    )
    if split_enabled and split_label and selected_groups:
        st.caption(
            f"已按{split_label}分面，展示销量前 {len(selected_groups)} 个分组。"
        )
    st.caption("建议结合 hover 阅读各价格段动总占比变化。")


def render_chart_sales_vs_price_scatter(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Sales vs Price Scatter",
        key_prefix="adv_sales_price",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    price_frame, msrp_col = build_price_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if price_frame.empty or not msrp_col:
        st.warning("缺少 MSRP 数据，无法绘制销量—价格散点图。")
        return

    model_df = price_frame.groupby(
        ["Segment", "Brand", "Model"],
        as_index=False,
    ).agg(
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    )
    model_df = model_df.sort_values("Sales", ascending=False)
    if model_df.empty:
        show_no_data("销量—价格散点")
        return

    segment_total = model_df.groupby("Segment")["Sales"].transform("sum")
    model_df["SegmentSharePct"] = (
        model_df["Sales"] / segment_total.replace(0, pd.NA)
    ).fillna(0.0) * 100

    top_n = 200
    with st.expander("高级设置", expanded=False):
        top_n = int(
            st.slider(
                "展示车型数（按销量）",
                min_value=30,
                max_value=500,
                value=top_n,
                step=10,
                key="adv_sales_price_topn",
            )
        )
    model_df = model_df.head(top_n)

    fig = px.scatter(
        model_df,
        x="MSRP",
        y="Sales",
        color="Segment",
        size="SegmentSharePct",
        hover_name="Model",
        hover_data={
            "Brand": True,
            "MSRP": ":,.0f",
            "Sales": ":,.0f",
            "SegmentSharePct": ":.2f",
        },
        title="Sales vs Price Scatter（销量—价格散点）",
        size_max=45,
    )
    fig = style_figure(fig)
    fig.update_xaxes(title=f"MSRP（{msrp_col}）")
    fig.update_yaxes(title="销量")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_sales_price_scatter",
        filename_prefix="sales_price_scatter",
    )


def render_chart_segment_share_by_length(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Segment Share by Length",
        key_prefix="adv_segment_length",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    if not columns.segment:
        st.warning("缺少 Segment 字段，无法绘制尺寸段份额图。")
        return

    vehicle_df, length_col, _ = build_vehicle_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if vehicle_df.empty or not length_col:
        st.warning("缺少车长或销量数据，无法绘制尺寸段份额图。")
        return

    band_size = 100
    with st.expander("高级设置", expanded=False):
        band_size = int(
            st.slider(
                "尺寸带宽（mm）",
                min_value=50,
                max_value=500,
                value=band_size,
                step=50,
                key="adv_segment_length_band_size",
            )
        )
    vehicle_df = vehicle_df.copy()
    vehicle_df["LengthBand"], band_order = make_length_bands(
        vehicle_df["Length"],
        band_size,
    )
    vehicle_df = vehicle_df.dropna(subset=["LengthBand"])
    if vehicle_df.empty:
        show_no_data("尺寸段份额")
        return

    share_df = vehicle_df.groupby(
        ["LengthBand", "Segment"],
        as_index=False,
    )["Sales"].sum()
    share_df["LengthBand"] = pd.Categorical(
        share_df["LengthBand"],
        categories=band_order,
        ordered=True,
    )
    share_df = share_df.sort_values("LengthBand")

    fig = px.bar(
        share_df,
        x="LengthBand",
        y="Sales",
        color="Segment",
        title="Segment Share by Length（尺寸段份额）",
    )
    fig.update_layout(barmode="stack")
    fig.update_traces(
        texttemplate="%{percentParent:.0%}",
        textposition="inside",
    )
    fig = style_figure(fig)
    fig.update_xaxes(title=f"车长分段（{length_col}）")
    fig.update_yaxes(title="销量")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_segment_share_length",
        filename_prefix="segment_share_length",
    )


def render_chart_estimated_tco_vs_msrp(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="Estimated TCO vs MSRP",
        key_prefix="adv_tco_msrp",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    price_frame, msrp_col = build_price_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if price_frame.empty or not msrp_col:
        st.warning("缺少 MSRP 数据，无法绘制估算 TCO 图。")
        return

    if not columns.powertrain:
        st.warning("缺少动总字段，无法按动总估算 TCO。")
        return

    with st.expander("高级设置：估算参数（当前数据无原始TCO字段）", expanded=False):
        p1, p2, p3 = st.columns(3)
        with p1:
            years = st.slider(
                "使用年限（年）",
                min_value=2,
                max_value=10,
                value=5,
                step=1,
                key="adv_tco_years",
            )
            annual_km = st.slider(
                "年里程（km）",
                min_value=8000,
                max_value=40000,
                value=15000,
                step=1000,
                key="adv_tco_annual_km",
            )
        with p2:
            depreciation_rate = st.slider(
                "折旧率（总占比）",
                min_value=0.20,
                max_value=0.80,
                value=0.50,
                step=0.01,
                key="adv_tco_depreciation_rate",
            )
            maintenance_rate = st.slider(
                "维保率（每年占MSRP）",
                min_value=0.005,
                max_value=0.060,
                value=0.018,
                step=0.001,
                key="adv_tco_maintenance_rate",
            )
        with p3:
            tax_insurance_rate = st.slider(
                "税费保险（每年占MSRP）",
                min_value=0.005,
                max_value=0.080,
                value=0.020,
                step=0.001,
                key="adv_tco_tax_insurance_rate",
            )
            energy_cost_base = st.slider(
                "能源成本基准（€/km）",
                min_value=0.03,
                max_value=0.30,
                value=0.10,
                step=0.01,
                key="adv_tco_energy_base",
            )

    model_df = price_frame.groupby(
        ["Segment", "Brand", "Model", "Powertrain"],
        as_index=False,
    ).agg(
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    )
    model_df = model_df.sort_values("Sales", ascending=False)
    if model_df.empty:
        show_no_data("估算TCO vs MSRP")
        return

    top_n = int(
        st.slider(
            "展示车型数（按销量）",
            min_value=30,
            max_value=500,
            value=180,
            step=10,
            key="adv_tco_topn",
        )
    )
    model_df = model_df.head(top_n)

    energy_factor_map = {
        "BEV": 0.55,
        "PHEV": 0.85,
        "HEV": 0.90,
        "MHEV": 1.00,
        "ICE": 1.10,
    }
    model_df = model_df.copy()
    model_df["EnergyFactor"] = model_df["Powertrain"].map(
        energy_factor_map
    ).fillna(1.0)
    usage_km = float(annual_km * years)
    model_df["EnergyCost"] = (
        usage_km * energy_cost_base * model_df["EnergyFactor"]
    )
    model_df["DepreciationCost"] = model_df["MSRP"] * depreciation_rate
    model_df["MaintenanceCost"] = (
        model_df["MSRP"] * maintenance_rate * years
    )
    model_df["TaxInsuranceCost"] = (
        model_df["MSRP"] * tax_insurance_rate * years
    )
    model_df["EstimatedTCO"] = (
        model_df["DepreciationCost"]
        + model_df["EnergyCost"]
        + model_df["MaintenanceCost"]
        + model_df["TaxInsuranceCost"]
    )

    fig = px.scatter(
        model_df,
        x="MSRP",
        y="EstimatedTCO",
        color="Powertrain",
        size="Sales",
        hover_name="Model",
        hover_data={
            "Brand": True,
            "Segment": True,
            "MSRP": ":,.0f",
            "EstimatedTCO": ":,.0f",
            "Sales": ":,.0f",
            "EnergyCost": ":,.0f",
            "DepreciationCost": ":,.0f",
        },
        title="Estimated TCO vs MSRP（估算TCO）",
        size_max=45,
        category_orders={"Powertrain": POWERTRAIN_DISPLAY_ORDER},
        color_discrete_map=POWERTRAIN_COLOR_MAP,
    )
    fig = style_figure(fig)
    fig.update_xaxes(title=f"MSRP（{msrp_col}）")
    fig.update_yaxes(title="Estimated TCO（€）")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_estimated_tco_msrp",
        filename_prefix="estimated_tco_msrp",
    )
    st.caption(
        "说明：该图基于可调参数进行估算，非财务口径TCO；用于相对比较。"
    )


def calculate_finance(
    msrp: float,
    down_percent: float,
    rv_percent: float,
    apr: float,
    term: int,
) -> tuple[float, float, float, float, float]:
    normalized_msrp = float(max(msrp, 0.0))
    normalized_term = max(int(term), 1)
    down_ratio = float(min(max(down_percent, 0.0), 100.0)) / 100.0
    rv_ratio = float(min(max(rv_percent, 0.0), 100.0)) / 100.0

    down_payment = normalized_msrp * down_ratio
    principal = max(normalized_msrp - down_payment, 0.0)
    rv = normalized_msrp * rv_ratio
    monthly_rate = float(max(apr, 0.0)) / 100.0 / 12.0

    if monthly_rate > 0:
        pv_rv = rv / ((1.0 + monthly_rate) ** normalized_term)
    else:
        pv_rv = rv

    # Clamp at zero to avoid negative payment scenarios in edge inputs.
    net_financed = max(principal - pv_rv, 0.0)
    if monthly_rate > 0:
        denominator = 1.0 - (1.0 + monthly_rate) ** (-normalized_term)
        pmt = net_financed * (monthly_rate / denominator)
    else:
        pmt = net_financed / normalized_term

    return pmt, principal, pv_rv, net_financed, rv


def resolve_country_finance_preset(
    country_name: str | None,
) -> dict[str, float]:
    default_preset = {
        "down_percent": 30.0,
        "rv_percent": 55.0,
        "apr": 5.0,
        "term": 36.0,
    }

    if not country_name:
        return default_preset

    preset_map = {
        "瑞典": {
            "down_percent": 20.0,
            "rv_percent": 56.0,
            "apr": 6.5,
            "term": 36.0,
        },
        "挪威": {
            "down_percent": 28.0,
            "rv_percent": 58.0,
            "apr": 4.2,
            "term": 36.0,
        },
        "德国": {
            "down_percent": 32.0,
            "rv_percent": 52.0,
            "apr": 4.5,
            "term": 36.0,
        },
        "荷兰": {
            "down_percent": 30.0,
            "rv_percent": 54.0,
            "apr": 4.6,
            "term": 36.0,
        },
        "英国": {
            "down_percent": 25.0,
            "rv_percent": 55.0,
            "apr": 5.5,
            "term": 48.0,
        },
        "法国": {
            "down_percent": 28.0,
            "rv_percent": 53.0,
            "apr": 4.7,
            "term": 36.0,
        },
    }

    return preset_map.get(str(country_name).strip(), default_preset)


def clamp_finance_preset(
    preset: dict[str, float],
) -> dict[str, float]:
    term_candidates = [24.0, 36.0, 48.0, 60.0]
    raw_term = float(preset.get("term", 36.0))
    normalized_term = min(
        term_candidates,
        key=lambda value: abs(value - raw_term),
    )
    return {
        "down_percent": float(
            min(max(preset.get("down_percent", 30.0), 0.0), 50.0)
        ),
        "rv_percent": float(
            min(max(preset.get("rv_percent", 55.0), 30.0), 70.0)
        ),
        "apr": float(
            min(max(preset.get("apr", 5.0), 0.0), 10.0)
        ),
        "term": float(normalized_term),
    }


def detect_primary_brand_model(
    price_frame: pd.DataFrame,
) -> tuple[str | None, str | None]:
    if price_frame.empty:
        return None, None

    invalid_tokens = {"", "nan", "未标注"}
    work_df = price_frame.copy()
    work_df["Brand"] = normalize_series(work_df["Brand"])
    work_df["Model"] = normalize_series(work_df["Model"])

    brand_rank = (
        work_df.groupby("Brand", as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
    )
    model_rank = (
        work_df.groupby("Model", as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
    )

    detected_brand = None
    for brand_value in brand_rank["Brand"].tolist():
        normalized = str(brand_value).strip()
        if normalized.lower() in invalid_tokens:
            continue
        detected_brand = normalized
        break

    detected_model = None
    for model_value in model_rank["Model"].tolist():
        normalized = str(model_value).strip()
        if normalized.lower() in invalid_tokens:
            continue
        detected_model = normalized
        break

    return detected_brand, detected_model


def resolve_msrp_ratio(
    price_frame: pd.DataFrame,
    column: str,
    target_value: str,
) -> float:
    if price_frame.empty:
        return 1.0

    target_series = normalize_series(price_frame[column])
    target_mask = target_series == str(target_value).strip()
    target_values = pd.to_numeric(
        price_frame.loc[target_mask, "MSRP"],
        errors="coerce",
    ).dropna()
    overall_values = pd.to_numeric(
        price_frame["MSRP"],
        errors="coerce",
    ).dropna()

    if target_values.empty or overall_values.empty:
        return 1.0

    overall_median = float(overall_values.median())
    if overall_median <= 0:
        return 1.0

    return float(target_values.median()) / overall_median


def resolve_brand_finance_preset(
    brand_name: str | None,
    country_preset: dict[str, float],
    price_frame: pd.DataFrame,
) -> dict[str, float]:
    base_preset = clamp_finance_preset(dict(country_preset))
    if not brand_name:
        return base_preset

    ratio = resolve_msrp_ratio(
        price_frame=price_frame,
        column="Brand",
        target_value=brand_name,
    )

    candidate = dict(base_preset)
    if ratio >= 1.20:
        candidate["down_percent"] += 4.0
        candidate["rv_percent"] += 3.0
        candidate["apr"] -= 0.2
    elif ratio <= 0.85:
        candidate["down_percent"] -= 4.0
        candidate["rv_percent"] -= 3.0
        candidate["apr"] += 0.2
        candidate["term"] = max(candidate["term"], 48.0)

    return clamp_finance_preset(candidate)


def resolve_model_finance_preset(
    model_name: str | None,
    brand_preset: dict[str, float],
    price_frame: pd.DataFrame,
) -> dict[str, float]:
    base_preset = clamp_finance_preset(dict(brand_preset))
    if not model_name:
        return base_preset

    ratio = resolve_msrp_ratio(
        price_frame=price_frame,
        column="Model",
        target_value=model_name,
    )

    candidate = dict(base_preset)
    if ratio >= 1.30:
        candidate["down_percent"] += 3.0
        candidate["rv_percent"] += 2.0
        candidate["apr"] -= 0.1
    elif ratio <= 0.80:
        candidate["down_percent"] -= 3.0
        candidate["rv_percent"] -= 2.0
        candidate["apr"] += 0.1
        candidate["term"] = max(candidate["term"], 48.0)

    return clamp_finance_preset(candidate)


def build_rv_preset_templates(
    country_preset: dict[str, float],
    brand_name: str | None = None,
    brand_preset: dict[str, float] | None = None,
    model_name: str | None = None,
    model_preset: dict[str, float] | None = None,
) -> dict[str, dict[str, float]]:
    balanced = clamp_finance_preset(dict(country_preset))
    conservative = {
        "down_percent": min(balanced["down_percent"] + 10.0, 50.0),
        "rv_percent": max(balanced["rv_percent"] - 6.0, 30.0),
        "apr": max(balanced["apr"] - 0.2, 0.0),
        "term": max(min(balanced["term"], 48.0), 24.0),
    }
    aggressive = {
        "down_percent": max(balanced["down_percent"] - 12.0, 0.0),
        "rv_percent": min(balanced["rv_percent"] + 6.0, 70.0),
        "apr": min(balanced["apr"] + 0.5, 10.0),
        "term": min(max(balanced["term"], 48.0), 60.0),
    }

    templates: dict[str, dict[str, float]] = {
        "平衡（国家默认）": balanced,
        "保守（高首付低残值）": clamp_finance_preset(conservative),
        "进取（低首付高残值）": clamp_finance_preset(aggressive),
    }

    if brand_name and brand_preset:
        templates[f"品牌默认（{brand_name}）"] = clamp_finance_preset(
            dict(brand_preset)
        )
    if model_name and model_preset:
        templates[f"车型默认（{model_name}）"] = clamp_finance_preset(
            dict(model_preset)
        )

    return templates


def build_default_rv_vehicle_rows(
    price_frame: pd.DataFrame,
    fallback_msrp: int,
    preset: dict[str, float],
    row_count: int = 3,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []

    if not price_frame.empty:
        candidates = (
            price_frame.groupby(["Brand", "Model"], as_index=False)
            .agg(
                MSRP=("MSRP", "median"),
                Sales=("Sales", "sum"),
            )
            .sort_values("Sales", ascending=False)
            .head(row_count)
        )
        for _, row in candidates.iterrows():
            brand = str(row["Brand"]).strip()
            model = str(row["Model"]).strip()
            vehicle_name = " ".join(
                part for part in [brand, model] if part and part != "nan"
            )
            if not vehicle_name:
                vehicle_name = f"车型{len(rows) + 1}"

            rows.append(
                {
                    "Vehicle": vehicle_name,
                    "MSRP (EUR)": int(max(float(row["MSRP"]), 0.0)),
                    "Down Payment (%)": float(preset["down_percent"]),
                    "Residual Value (%)": float(preset["rv_percent"]),
                    "APR (%)": float(preset["apr"]),
                    "Term (Months)": int(round(preset["term"])),
                }
            )

    while len(rows) < row_count:
        rows.append(
            {
                "Vehicle": f"车型{len(rows) + 1}",
                "MSRP (EUR)": int(max(fallback_msrp, 0)),
                "Down Payment (%)": float(preset["down_percent"]),
                "Residual Value (%)": float(preset["rv_percent"]),
                "APR (%)": float(preset["apr"]),
                "Term (Months)": int(round(preset["term"])),
            }
        )

    return pd.DataFrame(rows)


def render_chart_rv_finance_dashboard(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    msrp_input_column = "MSRP (EUR)"
    currency_rate_presets = {
        "EUR": 1.0,
        "SEK": 11.40,
        "NOK": 11.60,
        "DKK": 7.46,
        "GBP": 0.86,
        "USD": 1.09,
    }
    currency_state_key = "adv_rv_display_currency"
    fx_mode_state_key = "adv_rv_fx_mode"
    manual_rate_state_key = "adv_rv_manual_fx_rate"

    selection, selected_columns = get_time_selection_for_chart(
        chart_name="RV金融杠杆看板",
        key_prefix="adv_rv_finance",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    price_frame, msrp_col = build_price_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    msrp_default = 459_900
    if not price_frame.empty:
        median_msrp = pd.to_numeric(
            price_frame["MSRP"],
            errors="coerce",
        ).median()
        if pd.notna(median_msrp) and float(median_msrp) > 0:
            msrp_default = int(float(median_msrp))

    detected_country = None
    if columns.country and columns.country in filtered_df.columns:
        country_series = normalize_series(filtered_df[columns.country])
        if not country_series.empty:
            detected_country = str(country_series.mode(dropna=True).iloc[0])

    country_preset = resolve_country_finance_preset(detected_country)
    detected_brand, detected_model = detect_primary_brand_model(
        price_frame
    )
    brand_preset = resolve_brand_finance_preset(
        brand_name=detected_brand,
        country_preset=country_preset,
        price_frame=price_frame,
    )
    model_preset = resolve_model_finance_preset(
        model_name=detected_model,
        brand_preset=brand_preset,
        price_frame=price_frame,
    )
    preset_templates = build_rv_preset_templates(
        country_preset=country_preset,
        brand_name=detected_brand,
        brand_preset=brand_preset,
        model_name=detected_model,
        model_preset=model_preset,
    )
    default_template_name = "平衡（国家默认）"

    preset_state_key = "adv_rv_template_name"
    rows_state_key = "adv_rv_vehicle_rows"
    editor_state_key = "adv_rv_vehicle_editor"

    if preset_state_key not in st.session_state:
        st.session_state[preset_state_key] = default_template_name
    if st.session_state[preset_state_key] not in preset_templates:
        st.session_state[preset_state_key] = default_template_name

    if rows_state_key not in st.session_state:
        st.session_state[rows_state_key] = build_default_rv_vehicle_rows(
            price_frame=price_frame,
            fallback_msrp=msrp_default,
            preset=preset_templates[default_template_name],
            row_count=3,
        )

    if currency_state_key not in st.session_state:
        st.session_state[currency_state_key] = "EUR"
    if st.session_state[currency_state_key] not in currency_rate_presets:
        st.session_state[currency_state_key] = "EUR"
    if fx_mode_state_key not in st.session_state:
        st.session_state[fx_mode_state_key] = "预设汇率"

    st.caption("🚗 OMODA/JAECOO 金融杠杆可视化（RV 折现模型）")
    anchor_tokens = [f"国家={detected_country or '通用'}"]
    if detected_brand:
        anchor_tokens.append(f"品牌={detected_brand}")
    if detected_model:
        anchor_tokens.append(f"车型={detected_model}")
    st.caption("预设锚点：" + "｜".join(anchor_tokens))

    fx_col_1, fx_col_2, fx_col_3 = st.columns([1.4, 1.2, 1.4])
    with fx_col_1:
        display_currency = st.selectbox(
            "展示币种",
            options=list(currency_rate_presets.keys()),
            key=currency_state_key,
        )

    preset_fx_rate = float(currency_rate_presets[display_currency])
    with fx_col_2:
        fx_mode = st.radio(
            "汇率来源",
            options=["预设汇率", "手动输入"],
            horizontal=True,
            key=fx_mode_state_key,
        )

    if fx_mode == "手动输入":
        if manual_rate_state_key not in st.session_state:
            st.session_state[manual_rate_state_key] = preset_fx_rate
        with fx_col_3:
            fx_rate = float(
                st.number_input(
                    f"手动汇率（1 EUR = ? {display_currency}）",
                    min_value=0.0001,
                    value=float(st.session_state[manual_rate_state_key]),
                    step=0.0001,
                    format="%.4f",
                    key=manual_rate_state_key,
                )
            )
    else:
        fx_rate = preset_fx_rate
        st.session_state[manual_rate_state_key] = preset_fx_rate
        with fx_col_3:
            st.metric(
                "当前汇率",
                f"1 EUR = {fx_rate:.4f} {display_currency}",
            )

    msrp_display_column = f"MSRP ({display_currency})"
    monthly_payment_column = (
        f"Monthly Payment ({display_currency}/月)"
    )
    total_monthly_payments_column = (
        f"Total Monthly Payments ({display_currency})"
    )
    st.caption(
        "汇率口径："
        f"1 EUR = {fx_rate:.4f} {display_currency}"
        f"（{'手动输入' if fx_mode == '手动输入' else '预设汇率'}）。"
        "MSRP 输入列固定为 EUR。"
    )

    control_col_1, control_col_2, control_col_3 = st.columns([2, 1, 1])
    with control_col_1:
        selected_template_name = st.selectbox(
            "参数模板",
            options=list(preset_templates.keys()),
            key=preset_state_key,
        )
    with control_col_2:
        apply_template_clicked = st.button(
            "应用模板到全部车型",
            key="adv_rv_apply_template",
            width="stretch",
        )
    with control_col_3:
        reset_clicked = st.button(
            "重置参数",
            key="adv_rv_reset",
            width="stretch",
        )

    if reset_clicked:
        st.session_state[preset_state_key] = default_template_name
        st.session_state[rows_state_key] = build_default_rv_vehicle_rows(
            price_frame=price_frame,
            fallback_msrp=msrp_default,
            preset=preset_templates[default_template_name],
            row_count=3,
        )
        if editor_state_key in st.session_state:
            del st.session_state[editor_state_key]

    if apply_template_clicked:
        selected_preset = preset_templates[selected_template_name]
        rows_df = pd.DataFrame(st.session_state[rows_state_key]).copy()
        if rows_df.empty:
            rows_df = build_default_rv_vehicle_rows(
                price_frame=price_frame,
                fallback_msrp=msrp_default,
                preset=selected_preset,
                row_count=3,
            )
        rows_df["Down Payment (%)"] = float(selected_preset["down_percent"])
        rows_df["Residual Value (%)"] = float(selected_preset["rv_percent"])
        rows_df["APR (%)"] = float(selected_preset["apr"])
        rows_df["Term (Months)"] = int(round(selected_preset["term"]))
        st.session_state[rows_state_key] = rows_df
        if editor_state_key in st.session_state:
            del st.session_state[editor_state_key]

    st.caption(
        "可同时输入多车型参数。支持模板批量应用与重置。"
    )
    edited_rows_df = st.data_editor(
        pd.DataFrame(st.session_state[rows_state_key]),
        key=editor_state_key,
        num_rows="dynamic",
        width="stretch",
        hide_index=True,
        column_config={
            "Vehicle": st.column_config.TextColumn(
                "Vehicle",
                help="车型名称（可手填）",
                required=True,
            ),
            "MSRP (EUR)": st.column_config.NumberColumn(
                "MSRP (EUR)",
                min_value=0,
                step=1000,
                format="%d",
            ),
            "Down Payment (%)": st.column_config.NumberColumn(
                "Down Payment (%)",
                min_value=0,
                max_value=50,
                step=1,
                format="%.0f",
            ),
            "Residual Value (%)": st.column_config.NumberColumn(
                "Residual Value (%)",
                min_value=30,
                max_value=70,
                step=1,
                format="%.0f",
            ),
            "APR (%)": st.column_config.NumberColumn(
                "APR (%)",
                min_value=0.0,
                max_value=10.0,
                step=0.1,
                format="%.1f",
            ),
            "Term (Months)": st.column_config.NumberColumn(
                "Term (Months)",
                min_value=12,
                max_value=84,
                step=12,
                format="%d",
            ),
        },
    )
    st.session_state[rows_state_key] = pd.DataFrame(edited_rows_df).copy()

    working_rows = pd.DataFrame(st.session_state[rows_state_key]).copy()
    if working_rows.empty:
        st.warning("请至少输入 1 个车型后再进行 RV 计算。")
        return

    working_rows["Vehicle"] = (
        working_rows["Vehicle"].astype("string").fillna("").str.strip()
    )
    working_rows = working_rows[working_rows["Vehicle"] != ""].copy()
    if working_rows.empty:
        st.warning("存在空车型名称，请补充后再计算。")
        return

    numeric_columns = [
        msrp_input_column,
        "Down Payment (%)",
        "Residual Value (%)",
        "APR (%)",
        "Term (Months)",
    ]
    for column in numeric_columns:
        working_rows[column] = pd.to_numeric(
            working_rows[column],
            errors="coerce",
        )
    working_rows = working_rows.dropna(subset=numeric_columns).copy()
    if working_rows.empty:
        st.warning("未识别到可计算的参数行，请检查数值输入。")
        return

    result_rows: list[dict[str, float | int | str]] = []
    for _, row in working_rows.iterrows():
        msrp_value = float(row[msrp_input_column])
        term_months = int(row["Term (Months)"])
        pmt, principal, pv_rv, net_financed, rv = calculate_finance(
            msrp=msrp_value,
            down_percent=float(row["Down Payment (%)"]),
            rv_percent=float(row["Residual Value (%)"]),
            apr=float(row["APR (%)"]),
            term=term_months,
        )
        down_payment_amount_eur = max(msrp_value - principal, 0.0)
        total_monthly_payment = float(pmt * term_months)

        msrp_display_value = float(msrp_value * fx_rate)
        down_payment_amount = float(down_payment_amount_eur * fx_rate)
        principal_display = float(principal * fx_rate)
        pv_rv_display = float(pv_rv * fx_rate)
        net_financed_display = float(net_financed * fx_rate)
        balloon_payment_display = float(rv * fx_rate)
        monthly_payment_display = float(pmt * fx_rate)
        total_monthly_payment_display = float(total_monthly_payment * fx_rate)

        result_rows.append(
            {
                "Vehicle": str(row["Vehicle"]),
                msrp_display_column: msrp_display_value,
                "MSRP (EUR Base)": msrp_value,
                "Down Payment (%)": float(row["Down Payment (%)"]),
                "Residual Value (%)": float(row["Residual Value (%)"]),
                "APR (%)": float(row["APR (%)"]),
                "Term (Months)": term_months,
                "Down Payment Amount": float(down_payment_amount),
                "Principal": principal_display,
                "PV(RV)": pv_rv_display,
                "Net Financed": net_financed_display,
                "Balloon Payment": balloon_payment_display,
                monthly_payment_column: monthly_payment_display,
                total_monthly_payments_column: total_monthly_payment_display,
            }
        )

    result_df = pd.DataFrame(result_rows)
    if result_df.empty:
        st.warning("当前输入未产生有效结果。")
        return

    result_df = result_df.sort_values(
        monthly_payment_column,
        ascending=False,
    )
    summary_col_1, summary_col_2, summary_col_3 = st.columns(3)
    with summary_col_1:
        st.metric(
            "车型数",
            f"{len(result_df):,}",
        )
    with summary_col_2:
        st.metric(
            "月供均值",
            f"{result_df[monthly_payment_column].mean():,.0f}"
            f" {display_currency}/月",
        )
    with summary_col_3:
        st.metric(
            "月供最高",
            f"{result_df[monthly_payment_column].max():,.0f}"
            f" {display_currency}/月",
        )

    compare_fig = px.bar(
        result_df,
        x="Vehicle",
        y=monthly_payment_column,
        color="Term (Months)",
        title="多车型月供对比",
    )
    compare_fig.update_traces(
        texttemplate="%{y:,.0f}",
        textposition="outside",
    )
    compare_fig = style_figure(compare_fig)
    compare_fig.update_xaxes(title="车型")
    compare_fig.update_yaxes(title=monthly_payment_column)
    render_plotly_chart_with_png_export(
        fig=compare_fig,
        chart_key="adv_rv_finance_compare",
        filename_prefix="rv_finance_compare",
    )

    st.dataframe(
        result_df[
            [
                "Vehicle",
                msrp_display_column,
                "Down Payment (%)",
                "Residual Value (%)",
                "APR (%)",
                "Term (Months)",
                "Net Financed",
                monthly_payment_column,
                total_monthly_payments_column,
                "Balloon Payment",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    st.markdown("**方案参数同屏对比（A/B/C）**")
    vehicle_options = result_df["Vehicle"].astype(str).tolist()
    scenario_base_vehicle = st.selectbox(
        "基准车型（固定 MSRP）",
        options=vehicle_options,
        key="adv_rv_scheme_base_vehicle",
    )
    scenario_base_row = result_df[
        result_df["Vehicle"] == scenario_base_vehicle
    ].iloc[0]
    scenario_base_msrp_eur = float(scenario_base_row["MSRP (EUR Base)"])

    scenario_table_key = "adv_rv_scheme_param_table"
    scenario_table_base_key = "adv_rv_scheme_param_table_base"
    current_base_marker = (
        f"{scenario_base_vehicle}:{scenario_base_msrp_eur:.2f}"
    )

    def clamp_term_value(term_value: float) -> int:
        return int(min(max(round(term_value / 12) * 12, 12), 84))

    if (
        scenario_table_key not in st.session_state
        or st.session_state.get(scenario_table_base_key) != current_base_marker
    ):
        default_scenarios = pd.DataFrame(
            [
                {
                    "Scheme": "A",
                    "Down Payment (%)": float(
                        scenario_base_row["Down Payment (%)"]
                    ),
                    "Residual Value (%)": float(
                        scenario_base_row["Residual Value (%)"]
                    ),
                    "APR (%)": float(scenario_base_row["APR (%)"]),
                    "Term (Months)": int(scenario_base_row["Term (Months)"]),
                },
                {
                    "Scheme": "B",
                    "Down Payment (%)": float(
                        min(
                            max(
                                float(
                                    scenario_base_row[
                                        "Down Payment (%)"
                                    ]
                                )
                                + 5.0,
                                0.0,
                            ),
                            50.0,
                        )
                    ),
                    "Residual Value (%)": float(
                        min(
                            max(
                                float(
                                    scenario_base_row[
                                        "Residual Value (%)"
                                    ]
                                )
                                - 3.0,
                                30.0,
                            ),
                            70.0,
                        )
                    ),
                    "APR (%)": float(
                        min(
                            max(
                                float(scenario_base_row["APR (%)"]) + 0.5,
                                0.0,
                            ),
                            10.0,
                        )
                    ),
                    "Term (Months)": int(
                        scenario_base_row["Term (Months)"]
                    ),
                },
                {
                    "Scheme": "C",
                    "Down Payment (%)": float(
                        min(
                            max(
                                float(
                                    scenario_base_row[
                                        "Down Payment (%)"
                                    ]
                                )
                                - 5.0,
                                0.0,
                            ),
                            50.0,
                        )
                    ),
                    "Residual Value (%)": float(
                        min(
                            max(
                                float(
                                    scenario_base_row[
                                        "Residual Value (%)"
                                    ]
                                )
                                + 3.0,
                                30.0,
                            ),
                            70.0,
                        )
                    ),
                    "APR (%)": float(
                        min(
                            max(
                                float(scenario_base_row["APR (%)"]) - 0.5,
                                0.0,
                            ),
                            10.0,
                        )
                    ),
                    "Term (Months)": clamp_term_value(
                        float(scenario_base_row["Term (Months)"]) + 12.0
                    ),
                },
            ]
        )
        st.session_state[scenario_table_key] = default_scenarios
        st.session_state[scenario_table_base_key] = current_base_marker

    edited_scenario_df = st.data_editor(
        pd.DataFrame(st.session_state[scenario_table_key]),
        key="adv_rv_scheme_param_editor",
        num_rows="dynamic",
        width="stretch",
        hide_index=True,
        column_config={
            "Scheme": st.column_config.TextColumn(
                "Scheme",
                required=True,
            ),
            "Down Payment (%)": st.column_config.NumberColumn(
                "Down Payment (%)",
                min_value=0.0,
                max_value=50.0,
                step=1.0,
                format="%.1f",
            ),
            "Residual Value (%)": st.column_config.NumberColumn(
                "Residual Value (%)",
                min_value=30.0,
                max_value=70.0,
                step=1.0,
                format="%.1f",
            ),
            "APR (%)": st.column_config.NumberColumn(
                "APR (%)",
                min_value=0.0,
                max_value=10.0,
                step=0.1,
                format="%.2f",
            ),
            "Term (Months)": st.column_config.NumberColumn(
                "Term (Months)",
                min_value=12,
                max_value=84,
                step=12,
                format="%d",
            ),
        },
    )
    st.session_state[scenario_table_key] = pd.DataFrame(
        edited_scenario_df
    ).copy()

    scenario_param_df = pd.DataFrame(
        st.session_state[scenario_table_key]
    ).copy()
    scenario_param_df["Scheme"] = (
        scenario_param_df["Scheme"].astype("string").fillna("").str.strip()
    )
    scenario_param_df = scenario_param_df[
        scenario_param_df["Scheme"] != ""
    ].copy()

    if scenario_param_df.empty:
        st.info("请至少保留 2 个方案（A/B 或 A/B/C）。")
    else:
        for column_name in [
            "Down Payment (%)",
            "Residual Value (%)",
            "APR (%)",
            "Term (Months)",
        ]:
            scenario_param_df[column_name] = pd.to_numeric(
                scenario_param_df[column_name],
                errors="coerce",
            )

        scenario_param_df = scenario_param_df.dropna(
            subset=[
                "Down Payment (%)",
                "Residual Value (%)",
                "APR (%)",
                "Term (Months)",
            ]
        ).copy()

        if len(scenario_param_df) > 3:
            st.warning("最多对比 3 个方案，系统将仅保留前 3 行。")
            scenario_param_df = scenario_param_df.head(3).copy()

        if len(scenario_param_df) < 2:
            st.info("请至少输入 2 个有效方案参数。")
        else:
            scenario_rows: list[dict[str, float | int | str]] = []
            for _, scenario_row in scenario_param_df.iterrows():
                scenario_name = str(scenario_row["Scheme"])
                scenario_down = float(
                    min(max(scenario_row["Down Payment (%)"], 0.0), 50.0)
                )
                scenario_rv = float(
                    min(max(scenario_row["Residual Value (%)"], 30.0), 70.0)
                )
                scenario_apr = float(
                    min(max(scenario_row["APR (%)"], 0.0), 10.0)
                )
                scenario_term = clamp_term_value(
                    float(scenario_row["Term (Months)"])
                )

                (
                    scenario_pmt,
                    scenario_principal,
                    scenario_pv_rv,
                    scenario_net,
                    scenario_balloon,
                ) = calculate_finance(
                    msrp=scenario_base_msrp_eur,
                    down_percent=scenario_down,
                    rv_percent=scenario_rv,
                    apr=scenario_apr,
                    term=scenario_term,
                )
                scenario_down_payment = max(
                    scenario_base_msrp_eur - scenario_principal,
                    0.0,
                )
                scenario_rows.append(
                    {
                        "Scheme": scenario_name,
                        "Down Payment (%)": scenario_down,
                        "Residual Value (%)": scenario_rv,
                        "APR (%)": scenario_apr,
                        "Term (Months)": scenario_term,
                        "Net Financed": float(scenario_net * fx_rate),
                        monthly_payment_column: float(scenario_pmt * fx_rate),
                        total_monthly_payments_column: float(
                            scenario_pmt * scenario_term * fx_rate
                        ),
                        "Balloon Payment": float(
                            scenario_balloon * fx_rate
                        ),
                        "Down Payment Amount": float(
                            scenario_down_payment * fx_rate
                        ),
                        "PV(RV)": float(scenario_pv_rv * fx_rate),
                    }
                )

            scenario_result_df = pd.DataFrame(scenario_rows)
            scenario_result_df = scenario_result_df.drop_duplicates(
                subset=["Scheme"],
                keep="last",
            )

            st.caption(
                f"基准车型：{scenario_base_vehicle}"
                "｜固定 MSRP="
                f"{scenario_base_msrp_eur * fx_rate:,.0f}"
                f" {display_currency}"
            )

            scheme_cols = st.columns(len(scenario_result_df))
            for scheme_col, (_, scheme_row) in zip(
                scheme_cols,
                scenario_result_df.iterrows(),
            ):
                scheme_name = str(scheme_row["Scheme"])
                with scheme_col:
                    st.markdown(f"**方案 {scheme_name}**")
                    st.metric(
                        f"月供 ({display_currency}/月)",
                        f"{float(scheme_row[monthly_payment_column]):,.0f}",
                    )
                    st.metric(
                        f"净融资额 ({display_currency})",
                        f"{float(scheme_row['Net Financed']):,.0f}",
                    )
                    st.metric(
                        f"Balloon ({display_currency})",
                        f"{float(scheme_row['Balloon Payment']):,.0f}",
                    )

            scenario_bar_fig = px.bar(
                scenario_result_df,
                x="Scheme",
                y=monthly_payment_column,
                color="Scheme",
                title="方案月供同屏对比",
            )
            scenario_bar_fig.update_traces(
                texttemplate="%{y:,.0f}",
                textposition="outside",
            )
            scenario_bar_fig = style_figure(scenario_bar_fig)
            scenario_bar_fig.update_xaxes(title="方案")
            scenario_bar_fig.update_yaxes(title=monthly_payment_column)
            render_plotly_chart_with_png_export(
                fig=scenario_bar_fig,
                chart_key="adv_rv_scheme_compare",
                filename_prefix="rv_scheme_compare",
            )

            baseline_row = scenario_result_df.iloc[0]
            delta_rows: list[dict[str, str]] = []
            for _, scheme_row in scenario_result_df.iloc[1:].iterrows():
                baseline_monthly = float(baseline_row[monthly_payment_column])
                current_monthly = float(scheme_row[monthly_payment_column])
                baseline_net = float(baseline_row["Net Financed"])
                current_net = float(scheme_row["Net Financed"])

                monthly_delta = current_monthly - baseline_monthly
                net_delta = current_net - baseline_net
                monthly_ratio = (
                    monthly_delta / baseline_monthly
                    if baseline_monthly != 0
                    else float("nan")
                )
                net_ratio = (
                    net_delta / baseline_net
                    if baseline_net != 0
                    else float("nan")
                )

                delta_rows.append(
                    {
                        "Compare": f"{scheme_row['Scheme']}"
                        f" vs {baseline_row['Scheme']}",
                        "Monthly Delta": (
                            f"{monthly_delta:+,.0f} {display_currency}/月"
                        ),
                        "Monthly Delta %": (
                            f"{monthly_ratio:+.1%}"
                            if pd.notna(monthly_ratio)
                            else "N/A"
                        ),
                        "NetFin Delta": (
                            f"{net_delta:+,.0f} {display_currency}"
                        ),
                        "NetFin Delta %": (
                            f"{net_ratio:+.1%}"
                            if pd.notna(net_ratio)
                            else "N/A"
                        ),
                    }
                )

            if delta_rows:
                st.markdown("**A/B/C 差异摘要卡（参数方案）**")
                st.dataframe(
                    pd.DataFrame(delta_rows),
                    width="stretch",
                    hide_index=True,
                )
                st.caption(
                    f"基准方案：{baseline_row['Scheme']}"
                    "（第一行）。建议优先比较月供与净融资额差异。"
                )

    focus_vehicle = st.selectbox(
        "瀑布图展示车型",
        options=result_df["Vehicle"].tolist(),
        key="adv_rv_focus_vehicle",
    )
    focus_row = result_df[result_df["Vehicle"] == focus_vehicle].iloc[0]

    focus_monthly_payment = int(
        focus_row[monthly_payment_column]
    )
    st.subheader(
        f"{focus_vehicle} 月供预测: {focus_monthly_payment:,}"
        f" {display_currency} / 月"
    )

    waterfall_fig = go.Figure(
        go.Waterfall(
            name="Loan Logic",
            orientation="v",
            measure=[
                "absolute",
                "relative",
                "total",
                "relative",
                "total",
                "total",
            ],
            x=[
                "Total MSRP",
                "Down Payment",
                "P (Loan Principal)",
                "Minus PV(RV)",
                "Net Financed",
                "Monthly Payment",
            ],
            textposition="outside",
            text=[
                f"{int(focus_row[msrp_display_column]):,}",
                f"-{int(focus_row['Down Payment Amount']):,}",
                f"{int(focus_row['Principal']):,}",
                f"-{int(focus_row['PV(RV)']):,}",
                f"{int(focus_row['Net Financed']):,}",
                f"PMT: {int(focus_row[monthly_payment_column]):,}",
            ],
            y=[
                float(focus_row[msrp_display_column]),
                -float(focus_row["Down Payment Amount"]),
                float(focus_row["Principal"]),
                -float(focus_row["PV(RV)"]),
                float(focus_row["Net Financed"]),
                float(focus_row[monthly_payment_column]),
            ],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        )
    )
    waterfall_fig.update_layout(
        title="从贷款总额到月供的折算过程",
        showlegend=False,
    )
    render_plotly_chart_with_png_export(
        fig=style_figure(waterfall_fig),
        chart_key="adv_rv_finance_dashboard",
        filename_prefix="rv_finance_dashboard",
    )

    focus_term_months = int(focus_row["Term (Months)"])
    focus_apr_percent = float(focus_row["APR (%)"])
    focus_monthly_rate = focus_apr_percent / 100.0 / 12.0
    focus_msrp_amount = float(focus_row[msrp_display_column])
    focus_rv_amount = float(focus_row["Balloon Payment"])
    focus_pv_amount = float(focus_row["PV(RV)"])
    focus_down_payment = float(focus_row["Down Payment Amount"])
    focus_principal = float(focus_row["Principal"])
    focus_net_financed = float(focus_row["Net Financed"])
    focus_monthly_payment_amount = float(
        focus_row[monthly_payment_column]
    )

    curve_month_points = sorted(
        {
            12,
            24,
            36,
            48,
            60,
            max(focus_term_months, 1),
        }
    )
    pv_curve_rows: list[dict[str, float | int | str]] = []
    for month_value in curve_month_points:
        if focus_monthly_rate > 0:
            pv_value = focus_rv_amount / (
                (1.0 + focus_monthly_rate) ** month_value
            )
        else:
            pv_value = focus_rv_amount

        pv_curve_rows.append(
            {
                "Term (Months)": int(month_value),
                "Series": "Nominal RV",
                "Amount": float(focus_rv_amount),
            }
        )
        pv_curve_rows.append(
            {
                "Term (Months)": int(month_value),
                "Series": "PV(RV)",
                "Amount": float(pv_value),
            }
        )

    pv_curve_df = pd.DataFrame(pv_curve_rows)

    with st.expander("说明图：PV-RV关系与银行月供公式", expanded=False):
        relation_tab, formula_tab, sensitivity_tab = st.tabs(
            [
                "PV-RV关系图",
                "月供公式与步骤",
                "APR敏感性分析",
            ]
        )

        with relation_tab:
            relation_fig = px.line(
                pv_curve_df,
                x="Term (Months)",
                y="Amount",
                color="Series",
                markers=True,
                title=f"{focus_vehicle}：RV 与 PV(RV) 随期限变化",
            )
            relation_fig = style_figure(relation_fig)
            relation_fig.update_xaxes(title="Term (Months)")
            relation_fig.update_yaxes(title=f"Amount ({display_currency})")
            render_plotly_chart_with_png_export(
                fig=relation_fig,
                chart_key="adv_rv_pv_relation",
                filename_prefix="rv_pv_relation",
            )

            pv_discount_gap = max(focus_rv_amount - focus_pv_amount, 0.0)
            pv_ratio = (
                focus_pv_amount / focus_rv_amount
                if focus_rv_amount > 0
                else float("nan")
            )
            if pd.notna(pv_ratio):
                st.caption(
                    f"当前期限 {focus_term_months} 月："
                    f"PV(RV)={focus_pv_amount:,.0f} {display_currency}，"
                    f"折现差额={pv_discount_gap:,.0f} {display_currency}，"
                    f"PV/RV={pv_ratio:.1%}。"
                )

        with formula_tab:
            st.markdown("**银行月供计算公式**")
            st.latex(r"Down = MSRP \times down\%")
            st.latex(r"P = MSRP - Down")
            st.latex(r"PV(RV) = \frac{RV}{(1+r)^n}")
            st.latex(r"NetFinanced = P - PV(RV)")
            if focus_monthly_rate > 0:
                st.latex(
                    r"PMT = NetFinanced \times "
                    r"\frac{r}{1-(1+r)^{-n}}"
                )
            else:
                st.latex(r"PMT = \frac{NetFinanced}{n}")

            monthly_rate_percent = focus_monthly_rate * 100.0
            st.caption(
                f"APR 年化={focus_apr_percent:.2f}%"
                f" -> 月利率={monthly_rate_percent:.4f}%"
            )

            steps_df = pd.DataFrame(
                [
                    {
                        "Step": "1. Total MSRP",
                        "Formula": "输入",
                        "Value": (
                            f"{focus_msrp_amount:,.0f} {display_currency}"
                        ),
                    },
                    {
                        "Step": "2. Down Payment",
                        "Formula": "MSRP * down%",
                        "Value": (
                            f"{focus_down_payment:,.0f} {display_currency}"
                        ),
                    },
                    {
                        "Step": "3. P (Loan Principal)",
                        "Formula": "MSRP - Down",
                        "Value": (
                            f"{focus_principal:,.0f} {display_currency}"
                        ),
                    },
                    {
                        "Step": "4. PV(RV)",
                        "Formula": "RV / (1+r)^n",
                        "Value": (
                            f"{focus_pv_amount:,.0f} {display_currency}"
                        ),
                    },
                    {
                        "Step": "5. Net Financed",
                        "Formula": "P - PV(RV)",
                        "Value": (
                            f"{focus_net_financed:,.0f} {display_currency}"
                        ),
                    },
                    {
                        "Step": "6. Monthly Payment",
                        "Formula": "PMT(NetFinanced, r, n)",
                        "Value": (
                            f"{focus_monthly_payment_amount:,.0f} "
                            f"{display_currency}/月"
                        ),
                    },
                ]
            )
            st.dataframe(steps_df, width="stretch", hide_index=True)

            total_monthly_amount = (
                focus_monthly_payment_amount * focus_term_months
            )
            monthly_payment_share = (
                f"{focus_monthly_payment_amount / focus_msrp_amount:.1%}"
                if focus_msrp_amount > 0
                else "N/A"
            )
            total_monthly_share = (
                f"{total_monthly_amount / focus_msrp_amount:.1%}"
                if focus_msrp_amount > 0
                else "N/A"
            )

            composition_df = pd.DataFrame(
                [
                    {
                        "Component": "Total MSRP",
                        "Amount": focus_msrp_amount,
                        "Share of MSRP": (
                            "100.0%"
                            if focus_msrp_amount > 0
                            else "N/A"
                        ),
                    },
                    {
                        "Component": "Down Payment (-)",
                        "Amount": focus_down_payment,
                        "Share of MSRP": (
                            f"{focus_down_payment / focus_msrp_amount:.1%}"
                            if focus_msrp_amount > 0
                            else "N/A"
                        ),
                    },
                    {
                        "Component": "P (Loan Principal)",
                        "Amount": focus_principal,
                        "Share of MSRP": (
                            f"{focus_principal / focus_msrp_amount:.1%}"
                            if focus_msrp_amount > 0
                            else "N/A"
                        ),
                    },
                    {
                        "Component": "PV(RV) (-)",
                        "Amount": focus_pv_amount,
                        "Share of MSRP": (
                            f"{focus_pv_amount / focus_msrp_amount:.1%}"
                            if focus_msrp_amount > 0
                            else "N/A"
                        ),
                    },
                    {
                        "Component": "Net Financed",
                        "Amount": focus_net_financed,
                        "Share of MSRP": (
                            f"{focus_net_financed / focus_msrp_amount:.1%}"
                            if focus_msrp_amount > 0
                            else "N/A"
                        ),
                    },
                    {
                        "Component": "Monthly Payment",
                        "Amount": focus_monthly_payment_amount,
                        "Share of MSRP": monthly_payment_share,
                    },
                    {
                        "Component": "Total Monthly Payments",
                        "Amount": total_monthly_amount,
                        "Share of MSRP": total_monthly_share,
                    },
                ]
            )
            composition_df["Amount"] = composition_df["Amount"].map(
                lambda value: f"{float(value):,.0f} {display_currency}"
            )
            st.markdown("**净融资额组成明细**")
            st.dataframe(composition_df, width="stretch", hide_index=True)

            balloon_ratio = (
                focus_rv_amount / focus_msrp_amount
                if focus_msrp_amount > 0
                else 0.0
            )
            if balloon_ratio >= 0.55:
                st.warning(
                    f"Balloon Payment 占 MSRP 比例为 {balloon_ratio:.1%}，"
                    "到期一次性支付压力较高，请评估再融资或置换策略。"
                )
            elif balloon_ratio >= 0.40:
                st.info(
                    f"Balloon Payment 占 MSRP 比例为 {balloon_ratio:.1%}，"
                    "建议关注到期现金流安排。"
                )

        with sensitivity_tab:
            st.markdown("**参数敏感性分析（其他参数固定）**")
            base_msrp_eur = (
                focus_msrp_amount / fx_rate if fx_rate > 0 else 0.0
            )
            base_down = float(focus_row["Down Payment (%)"])
            base_rv = float(focus_row["Residual Value (%)"])
            base_apr = float(focus_row["APR (%)"])
            base_term = int(focus_row["Term (Months)"])
            st.caption(
                "基准参数："
                f"首付 {base_down:.0f}%｜残值 {base_rv:.0f}%｜"
                f"APR {base_apr:.2f}%｜期限 {base_term} 月"
            )

            apr_tab, rv_tab, down_tab, term_tab = st.tabs(
                ["APR", "RV(残值率)", "首付", "期限"]
            )

            with apr_tab:
                apr_col_1, apr_col_2 = st.columns(2)
                with apr_col_1:
                    apr_span = st.slider(
                        "APR 扰动范围（±百分点）",
                        min_value=0.5,
                        max_value=4.0,
                        value=2.0,
                        step=0.1,
                        key="adv_rv_apr_span",
                    )
                with apr_col_2:
                    apr_points = st.slider(
                        "采样点数",
                        min_value=5,
                        max_value=25,
                        value=11,
                        step=2,
                        key="adv_rv_apr_points",
                    )

                min_apr = max(0.0, base_apr - apr_span)
                max_apr = min(15.0, base_apr + apr_span)
                if apr_points <= 1 or max_apr <= min_apr:
                    apr_grid = [base_apr]
                else:
                    apr_step = (max_apr - min_apr) / (apr_points - 1)
                    apr_grid = [
                        min_apr + index * apr_step
                        for index in range(apr_points)
                    ]

                apr_rows: list[dict[str, float]] = []
                for apr_value in apr_grid:
                    scenario_pmt, _, _, _, _ = calculate_finance(
                        msrp=base_msrp_eur,
                        down_percent=base_down,
                        rv_percent=base_rv,
                        apr=float(apr_value),
                        term=base_term,
                    )
                    apr_rows.append(
                        {
                            "APR (%)": float(apr_value),
                            monthly_payment_column: float(
                                scenario_pmt * fx_rate
                            ),
                        }
                    )

                apr_df = pd.DataFrame(apr_rows)
                apr_fig = px.line(
                    apr_df,
                    x="APR (%)",
                    y=monthly_payment_column,
                    markers=True,
                    title=f"{focus_vehicle}：APR 对月供影响",
                )
                apr_fig = style_figure(apr_fig)
                apr_fig.update_xaxes(title="APR (%)")
                apr_fig.update_yaxes(title=monthly_payment_column)
                apr_fig.add_vline(
                    x=base_apr,
                    line_dash="dash",
                    line_color="#6B7280",
                )
                render_plotly_chart_with_png_export(
                    fig=apr_fig,
                    chart_key="adv_rv_apr_sensitivity",
                    filename_prefix="rv_apr_sensitivity",
                )

            with rv_tab:
                rv_col_1, rv_col_2 = st.columns(2)
                with rv_col_1:
                    rv_span = st.slider(
                        "残值率扰动范围（±百分点）",
                        min_value=2.0,
                        max_value=20.0,
                        value=8.0,
                        step=1.0,
                        key="adv_rv_pct_span",
                    )
                with rv_col_2:
                    rv_points = st.slider(
                        "采样点数",
                        min_value=5,
                        max_value=25,
                        value=11,
                        step=2,
                        key="adv_rv_pct_points",
                    )

                min_rv = max(30.0, base_rv - rv_span)
                max_rv = min(70.0, base_rv + rv_span)
                if rv_points <= 1 or max_rv <= min_rv:
                    rv_grid = [base_rv]
                else:
                    rv_step = (max_rv - min_rv) / (rv_points - 1)
                    rv_grid = [
                        min_rv + index * rv_step
                        for index in range(rv_points)
                    ]

                rv_rows: list[dict[str, float]] = []
                for rv_value in rv_grid:
                    scenario_pmt, _, _, _, _ = calculate_finance(
                        msrp=base_msrp_eur,
                        down_percent=base_down,
                        rv_percent=float(rv_value),
                        apr=base_apr,
                        term=base_term,
                    )
                    rv_rows.append(
                        {
                            "RV (%)": float(rv_value),
                            monthly_payment_column: float(
                                scenario_pmt * fx_rate
                            ),
                        }
                    )

                rv_df = pd.DataFrame(rv_rows)
                rv_fig = px.line(
                    rv_df,
                    x="RV (%)",
                    y=monthly_payment_column,
                    markers=True,
                    title=f"{focus_vehicle}：残值率 对月供影响",
                )
                rv_fig = style_figure(rv_fig)
                rv_fig.update_xaxes(title="RV (%)")
                rv_fig.update_yaxes(title=monthly_payment_column)
                rv_fig.add_vline(
                    x=base_rv,
                    line_dash="dash",
                    line_color="#6B7280",
                )
                render_plotly_chart_with_png_export(
                    fig=rv_fig,
                    chart_key="adv_rv_pct_sensitivity",
                    filename_prefix="rv_pct_sensitivity",
                )

            with down_tab:
                down_col_1, down_col_2 = st.columns(2)
                with down_col_1:
                    down_span = st.slider(
                        "首付比例扰动范围（±百分点）",
                        min_value=2.0,
                        max_value=25.0,
                        value=10.0,
                        step=1.0,
                        key="adv_rv_down_span",
                    )
                with down_col_2:
                    down_points = st.slider(
                        "采样点数",
                        min_value=5,
                        max_value=25,
                        value=11,
                        step=2,
                        key="adv_rv_down_points",
                    )

                min_down = max(0.0, base_down - down_span)
                max_down = min(50.0, base_down + down_span)
                if down_points <= 1 or max_down <= min_down:
                    down_grid = [base_down]
                else:
                    down_step = (max_down - min_down) / (down_points - 1)
                    down_grid = [
                        min_down + index * down_step
                        for index in range(down_points)
                    ]

                down_rows: list[dict[str, float]] = []
                for down_value in down_grid:
                    scenario_pmt, _, _, _, _ = calculate_finance(
                        msrp=base_msrp_eur,
                        down_percent=float(down_value),
                        rv_percent=base_rv,
                        apr=base_apr,
                        term=base_term,
                    )
                    down_rows.append(
                        {
                            "Down Payment (%)": float(down_value),
                            monthly_payment_column: float(
                                scenario_pmt * fx_rate
                            ),
                        }
                    )

                down_df = pd.DataFrame(down_rows)
                down_fig = px.line(
                    down_df,
                    x="Down Payment (%)",
                    y=monthly_payment_column,
                    markers=True,
                    title=f"{focus_vehicle}：首付比例 对月供影响",
                )
                down_fig = style_figure(down_fig)
                down_fig.update_xaxes(title="Down Payment (%)")
                down_fig.update_yaxes(title=monthly_payment_column)
                down_fig.add_vline(
                    x=base_down,
                    line_dash="dash",
                    line_color="#6B7280",
                )
                render_plotly_chart_with_png_export(
                    fig=down_fig,
                    chart_key="adv_rv_down_sensitivity",
                    filename_prefix="rv_down_sensitivity",
                )

            with term_tab:
                term_options = [24, 36, 48, 60, 72, 84]
                default_terms = sorted(
                    {
                        24,
                        36,
                        48,
                        60,
                        int(base_term),
                    }
                )
                selected_terms = st.multiselect(
                    "期限采样（Months）",
                    options=term_options,
                    default=[
                        term
                        for term in default_terms
                        if term in term_options
                    ],
                    key="adv_rv_term_sensitivity_terms",
                )
                if not selected_terms:
                    st.info("请至少选择 1 个期限点。")
                else:
                    term_rows: list[dict[str, float | int]] = []
                    for term_value in sorted(selected_terms):
                        scenario_pmt, _, _, _, _ = calculate_finance(
                            msrp=base_msrp_eur,
                            down_percent=base_down,
                            rv_percent=base_rv,
                            apr=base_apr,
                            term=int(term_value),
                        )
                        term_rows.append(
                            {
                                "Term (Months)": int(term_value),
                                monthly_payment_column: float(
                                    scenario_pmt * fx_rate
                                ),
                            }
                        )

                    term_df = pd.DataFrame(term_rows)
                    term_fig = px.line(
                        term_df,
                        x="Term (Months)",
                        y=monthly_payment_column,
                        markers=True,
                        title=f"{focus_vehicle}：期限 对月供影响",
                    )
                    term_fig = style_figure(term_fig)
                    term_fig.update_xaxes(title="Term (Months)")
                    term_fig.update_yaxes(title=monthly_payment_column)
                    term_fig.add_vline(
                        x=base_term,
                        line_dash="dash",
                        line_color="#6B7280",
                    )
                    render_plotly_chart_with_png_export(
                        fig=term_fig,
                        chart_key="adv_rv_term_sensitivity",
                        filename_prefix="rv_term_sensitivity",
                    )

            st.markdown("**月供区间 Tornado 图**")
            tornado_rows: list[dict[str, float | str]] = []

            apr_low = max(0.0, base_apr - 2.0)
            apr_high = min(15.0, base_apr + 2.0)
            pmt_apr_low, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=base_rv,
                apr=apr_low,
                term=base_term,
            )
            pmt_apr_high, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=base_rv,
                apr=apr_high,
                term=base_term,
            )
            tornado_rows.append(
                {
                    "Parameter": "APR",
                    "Low": float(pmt_apr_low * fx_rate),
                    "High": float(pmt_apr_high * fx_rate),
                }
            )

            rv_low = max(30.0, base_rv - 8.0)
            rv_high = min(70.0, base_rv + 8.0)
            pmt_rv_low, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=rv_low,
                apr=base_apr,
                term=base_term,
            )
            pmt_rv_high, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=rv_high,
                apr=base_apr,
                term=base_term,
            )
            tornado_rows.append(
                {
                    "Parameter": "RV",
                    "Low": float(pmt_rv_low * fx_rate),
                    "High": float(pmt_rv_high * fx_rate),
                }
            )

            down_low = max(0.0, base_down - 10.0)
            down_high = min(50.0, base_down + 10.0)
            pmt_down_low, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=down_low,
                rv_percent=base_rv,
                apr=base_apr,
                term=base_term,
            )
            pmt_down_high, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=down_high,
                rv_percent=base_rv,
                apr=base_apr,
                term=base_term,
            )
            tornado_rows.append(
                {
                    "Parameter": "Down Payment",
                    "Low": float(pmt_down_low * fx_rate),
                    "High": float(pmt_down_high * fx_rate),
                }
            )

            term_low = max(24, base_term - 12)
            term_high = min(84, base_term + 12)
            pmt_term_low, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=base_rv,
                apr=base_apr,
                term=term_low,
            )
            pmt_term_high, _, _, _, _ = calculate_finance(
                msrp=base_msrp_eur,
                down_percent=base_down,
                rv_percent=base_rv,
                apr=base_apr,
                term=term_high,
            )
            tornado_rows.append(
                {
                    "Parameter": "Term",
                    "Low": float(pmt_term_low * fx_rate),
                    "High": float(pmt_term_high * fx_rate),
                }
            )

            tornado_df = pd.DataFrame(tornado_rows)
            tornado_df["Range"] = (
                tornado_df[["Low", "High"]].max(axis=1)
                - tornado_df[["Low", "High"]].min(axis=1)
            )
            tornado_df = tornado_df.sort_values("Range", ascending=True)

            tornado_fig = go.Figure()
            tornado_fig.add_trace(
                go.Bar(
                    y=tornado_df["Parameter"],
                    x=tornado_df["Low"],
                    name="Low",
                    orientation="h",
                    marker_color="#93C5FD",
                )
            )
            tornado_fig.add_trace(
                go.Bar(
                    y=tornado_df["Parameter"],
                    x=tornado_df["High"],
                    name="High",
                    orientation="h",
                    marker_color="#1D4ED8",
                )
            )
            tornado_fig.update_layout(
                title="月供区间 Tornado（参数扰动）",
                barmode="overlay",
                showlegend=True,
            )
            tornado_fig = style_figure(tornado_fig)
            tornado_fig.update_xaxes(title=monthly_payment_column)
            tornado_fig.update_yaxes(title="参数")
            render_plotly_chart_with_png_export(
                fig=tornado_fig,
                chart_key="adv_rv_tornado",
                filename_prefix="rv_tornado",
            )

            st.markdown("**PMT 等高线图（APR × RV）**")
            contour_apr_grid = [
                max(0.0, base_apr - 2.0) + index * 0.5
                for index in range(9)
            ]
            contour_rv_grid = [
                max(30.0, base_rv - 12.0) + index * 3.0
                for index in range(9)
            ]

            z_values: list[list[float]] = []
            for rv_value in contour_rv_grid:
                row_values: list[float] = []
                for apr_value in contour_apr_grid:
                    pmt_value, _, _, _, _ = calculate_finance(
                        msrp=base_msrp_eur,
                        down_percent=base_down,
                        rv_percent=min(max(rv_value, 30.0), 70.0),
                        apr=apr_value,
                        term=base_term,
                    )
                    row_values.append(float(pmt_value * fx_rate))
                z_values.append(row_values)

            contour_fig = go.Figure(
                data=go.Contour(
                    z=z_values,
                    x=contour_apr_grid,
                    y=contour_rv_grid,
                    colorscale="Blues",
                    contours=dict(showlabels=True),
                    colorbar=dict(title=monthly_payment_column),
                )
            )
            contour_fig.update_layout(
                title=f"{focus_vehicle}：PMT 等高线（固定首付与期限）",
            )
            contour_fig = style_figure(contour_fig)
            contour_fig.update_xaxes(title="APR (%)")
            contour_fig.update_yaxes(title="RV (%)")
            render_plotly_chart_with_png_export(
                fig=contour_fig,
                chart_key="adv_rv_pmt_contour",
                filename_prefix="rv_pmt_contour",
            )

    st.caption(
        f"口径说明：Monthly Payment 为“每月金额（{display_currency}/月）”，"
        "在瀑布图中用独立 total 柱展示，不与 Net Financed 做同层累计。"
    )
    st.caption(
        "关系说明：Monthly Payment = PMT(Net Financed, APR, Term)，"
        "Total Monthly Payments = Monthly Payment * Term。"
    )

    if float(focus_row["Net Financed"]) <= 0:
        st.warning(
            "当前参数下净融资额为 0（或接近 0），请下调首付/残值比例后再观察月供变化。"
        )

    st.info(
        "💡 洞察：银行实际上只要求你为 "
        f"{int(focus_row['Net Financed']):,} {display_currency} 的差额支付 "
        f"{int(focus_row['Term (Months)']):,} 个月月供，"
        f"剩余的 {int(focus_row['Balloon Payment']):,} {display_currency} "
        "将作为未来 Balloon Payment。"
    )
    active_template_name = str(
        st.session_state.get(preset_state_key, default_template_name)
    )
    active_preset = preset_templates.get(
        active_template_name,
        clamp_finance_preset(country_preset),
    )
    st.caption(
        f"当前模板：{active_template_name}"
        f"｜首付 {active_preset['down_percent']:.0f}%"
        f"｜残值 {active_preset['rv_percent']:.0f}%"
        f"｜APR {active_preset['apr']:.1f}%"
        f"｜期限 {int(active_preset['term'])} 月"
    )
    if msrp_col:
        st.caption(
            f"默认 MSRP 来自当前筛选时间窗的中位数（字段：{msrp_col}）。"
        )


def render_chart_powertrain_bubble(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    if not columns.model or not columns.powertrain:
        st.warning("缺少 Model 或 动总规整 字段，无法绘制气泡图。")
        return

    selection, selected_time_columns = get_time_selection_for_chart(
        chart_name="动总分布气泡图",
        key_prefix="bubble_chart",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    year_columns = get_year_columns(filtered_df)
    sales_ref = sum_sales_for_columns(
        filtered_df,
        selected_time_columns,
    )

    length_col, length_values = prepare_numeric_axis(
        filtered_df,
        list(LENGTH_CANDIDATES),
    )
    msrp_col, msrp_values = prepare_numeric_axis(
        filtered_df,
        list(MSRP_CANDIDATES),
    )

    if not length_col or not msrp_col:
        st.warning("缺少可用的车长或 MSRP 数值列，无法绘制气泡图。")
        return

    if columns.make:
        brand_series = normalize_series(filtered_df[columns.make])
    else:
        brand_series = pd.Series("全部品牌", index=filtered_df.index)

    bubble_raw = pd.DataFrame(
        {
            "Model": normalize_series(filtered_df[columns.model]),
            "Brand": brand_series,
            "Powertrain": normalize_series(filtered_df[columns.powertrain]),
            "Length": length_values,
            "MSRP": msrp_values,
            "Sales": sales_ref,
        }
    )
    bubble_raw = bubble_raw.dropna(subset=["Length", "MSRP", "Sales"])
    bubble_raw = bubble_raw[
        bubble_raw["Powertrain"].isin(POWERTRAIN_DISPLAY_ORDER)
    ]

    if bubble_raw.empty:
        st.info("当前筛选下无 BEV/MHEV/PHEV/ICE/HEV 数据。")
        return

    with st.container(border=True):
        st.caption("图表控制")
        core_col_1, core_col_2, core_col_3 = st.columns([1, 1, 1])
        with core_col_1:
            top_n_models = int(
                st.number_input(
                    "Top N Model（气泡图）",
                    min_value=5,
                    max_value=80,
                    value=25,
                    step=1,
                    key="bubble_top_n_models",
                )
            )

        with core_col_2:
            facet_by_brand = st.checkbox(
                "按品牌分面对比",
                value=False,
                key="bubble_facet_brand",
                disabled=not bool(columns.make),
            )

        bubble_size_multiplier = 1
        with core_col_3:
            bubble_size_boost = st.checkbox(
                "气泡倍率放大",
                value=False,
                key="bubble_size_boost",
            )
            if bubble_size_boost:
                bubble_size_multiplier = int(
                    st.select_slider(
                        "放大倍数",
                        options=[2, 3, 4],
                        value=2,
                        key="bubble_size_multiplier",
                    )
                )
        max_brand_facets = 4
        show_yoy_label = True
        yoy_compare_year: str | None = None
        yoy_base_year: str | None = None
        year_options = [str(year) for year in year_columns]

        with st.expander("高级设置", expanded=False):
            if facet_by_brand:
                max_brand_facets = int(
                    st.number_input(
                        "最多展示品牌数",
                        min_value=2,
                        max_value=12,
                        value=4,
                        step=1,
                        key="bubble_facet_brand_top",
                    )
                )
            else:
                st.caption("开启“按品牌分面对比”后可设置品牌数。")

            yoy_col_1, yoy_col_2 = st.columns([1, 2])
            with yoy_col_1:
                show_yoy_label = st.checkbox(
                    "hover显示YoY",
                    value=True,
                    key="bubble_show_yoy_label",
                )

            with yoy_col_2:
                if show_yoy_label:
                    if len(year_options) < 2:
                        st.warning("年度列不足两年，无法显示 YoY。")
                        show_yoy_label = False
                    else:
                        compare_options = year_options[1:]
                        yoy_compare_year = st.selectbox(
                            "YoY 年份",
                            compare_options,
                            index=len(compare_options) - 1,
                            key="bubble_yoy_compare_year",
                        )
                        compare_idx = year_options.index(yoy_compare_year)
                        yoy_base_year = year_options[compare_idx - 1]
                        st.caption(
                            f"YoY 基准：{yoy_base_year} → {yoy_compare_year}"
                        )

    model_rank = (
        bubble_raw.groupby("Model", as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
    )
    top_models = set(model_rank.head(top_n_models)["Model"])
    bubble_raw = bubble_raw[bubble_raw["Model"].isin(top_models)]

    if show_yoy_label and yoy_compare_year and yoy_base_year:
        bubble_raw["SalesCurrent"] = pd.to_numeric(
            filtered_df.loc[bubble_raw.index, yoy_compare_year],
            errors="coerce",
        ).fillna(0.0)
        bubble_raw["SalesBase"] = pd.to_numeric(
            filtered_df.loc[bubble_raw.index, yoy_base_year],
            errors="coerce",
        ).fillna(0.0)
    else:
        bubble_raw["SalesCurrent"] = bubble_raw["Sales"]
        bubble_raw["SalesBase"] = bubble_raw["Sales"]

    facet_col = None
    category_orders = {"Powertrain": POWERTRAIN_DISPLAY_ORDER}
    if facet_by_brand and columns.make:
        brand_rank = (
            bubble_raw.groupby("Brand", as_index=False)["Sales"]
            .sum()
            .sort_values("Sales", ascending=False)
        )
        selected_brands = (
            brand_rank.head(max_brand_facets)["Brand"].astype(str).tolist()
        )
        bubble_raw = bubble_raw[bubble_raw["Brand"].isin(selected_brands)]
        category_orders["Brand"] = selected_brands
        facet_col = "Brand"

    group_columns = ["Model", "Powertrain"]
    if facet_col:
        group_columns.append("Brand")

    bubble_df = bubble_raw.groupby(group_columns, as_index=False).agg(
        Length=("Length", "median"),
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
        SalesCurrent=("SalesCurrent", "sum"),
        SalesBase=("SalesBase", "sum"),
    )

    if show_yoy_label and yoy_compare_year and yoy_base_year:
        base_sales = pd.to_numeric(bubble_df["SalesBase"], errors="coerce")
        base_sales = base_sales.where(base_sales != 0)
        current_sales = pd.to_numeric(
            bubble_df["SalesCurrent"],
            errors="coerce",
        )
        yoy_ratio = (current_sales - base_sales).div(base_sales)
        yoy_ratio = pd.to_numeric(yoy_ratio, errors="coerce")
        yoy_ratio = yoy_ratio.fillna(0.0).clip(-0.8, 3.0)
        bubble_df["YoYPct"] = yoy_ratio * 100
    else:
        bubble_df["YoYPct"] = 0.0

    size_multiplier = bubble_size_multiplier if bubble_size_boost else 1
    bubble_visual_scale = float(size_multiplier)
    bubble_df["BubbleSize"] = bubble_df["Sales"].clip(lower=1)
    bubble_df["SizeMultiplier"] = size_multiplier
    bubble_df["BubbleVisualScale"] = bubble_visual_scale
    size_max = int(24 * bubble_visual_scale)
    size_max = max(8, min(size_max, 140))

    if bubble_df.empty:
        show_no_data("动总分布气泡图")
        return

    chart_title = "筛选后 Model 动总分布气泡图"
    if facet_col:
        chart_title = "筛选后 Model 动总分布气泡图（按品牌分面）"

    scatter_kwargs: dict[str, str | int] = {}
    if facet_col:
        scatter_kwargs["facet_col"] = "Brand"
        scatter_kwargs["facet_col_wrap"] = 2

    bubble_hover_data = {
        "Sales": ":,.0f",
        "Length": ":,.0f",
        "MSRP": ":,.0f",
        "BubbleSize": False,
        "SizeMultiplier": False,
        "BubbleVisualScale": False,
        "SalesCurrent": False,
        "SalesBase": False,
        "YoYPct": ":.1f" if show_yoy_label else False,
    }

    fig_bubble = px.scatter(
        bubble_df,
        x="Length",
        y="MSRP",
        size="BubbleSize",
        size_max=size_max,
        color="Powertrain",
        hover_name="Model",
        hover_data=bubble_hover_data,
        category_orders=category_orders,
        color_discrete_map=POWERTRAIN_COLOR_MAP,
        title=chart_title,
        **scatter_kwargs,
    )
    if facet_col:
        fig_bubble.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace("Brand=", "")
            )
        )
    fig_bubble = style_figure(fig_bubble)
    fig_bubble.update_xaxes(title=f"车长（{length_col}）")
    fig_bubble.update_yaxes(title=f"MSRP（{msrp_col}）")

    if facet_col:
        st.caption(f"按品牌分面显示前 {max_brand_facets} 个品牌。")
    st.caption("仅显示 BEV/MHEV/PHEV/ICE/HEV；其余动总类型不显示。")
    if show_yoy_label and yoy_compare_year and yoy_base_year:
        st.caption(
            "hover显示 Model 与 YoY；颜色=动总类型；"
            "气泡大小=销量×放大倍数；"
            "Sales 数值本身不变，仅视觉放大。"
        )
    else:
        st.caption(
            "hover显示 Model；颜色=动总类型；"
            "气泡大小=销量×放大倍数；"
            "Sales 数值本身不变，仅视觉放大。"
        )
    if bubble_size_boost:
        st.caption(f"当前气泡放大倍数：{bubble_size_multiplier}x")
    else:
        st.caption("当前气泡放大倍数：1x")
    render_plotly_chart_with_png_export(
        fig=fig_bubble,
        chart_key="adv_powertrain_bubble",
        filename_prefix="powertrain_bubble",
    )


def resolve_year_column(
    filtered_df: pd.DataFrame,
    year_text: str,
) -> str | None:
    for column in get_year_columns(filtered_df):
        if str(column).strip() == str(year_text).strip():
            return str(column)
    return None


def build_nev_base_frame(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selected_columns: list[str],
) -> tuple[pd.DataFrame, str | None]:
    if not columns.powertrain:
        return pd.DataFrame(), None

    range_col, range_values = prepare_numeric_axis(
        filtered_df,
        list(BATTERY_RANGE_CANDIDATES),
    )
    if not range_col:
        return pd.DataFrame(), None

    sales_window = sum_sales_for_columns(filtered_df, selected_columns)
    model_series = (
        normalize_series(filtered_df[columns.model])
        if columns.model
        else pd.Series("未标注", index=filtered_df.index)
    )
    brand_series = (
        normalize_series(filtered_df[columns.make])
        if columns.make
        else pd.Series("全部品牌", index=filtered_df.index)
    )

    frame = pd.DataFrame(
        {
            "Model": model_series,
            "Brand": brand_series,
            "Powertrain": normalize_powertrain_for_nev(
                filtered_df[columns.powertrain]
            ),
            "BatteryRange": range_values,
            "SalesWindow": pd.to_numeric(
                sales_window,
                errors="coerce",
            ).fillna(0.0),
        }
    )
    frame = frame.dropna(subset=["BatteryRange"])
    frame = frame[frame["BatteryRange"] >= 0]
    return frame, range_col


def render_chart_nev_range_distribution(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="NEV续航分布",
        key_prefix="adv_nev_range",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return
    if not columns.powertrain:
        st.warning("缺少动总字段，无法绘制 NEV 续航分布。")
        return

    nev_pt_rows = int(
        normalize_powertrain_for_nev(
            filtered_df[columns.powertrain]
        ).isin(["BEV", "PHEV"]).sum()
    )

    nev_df, range_col = build_nev_base_frame(
        filtered_df,
        columns,
        selected_columns,
    )
    if not range_col:
        st.warning(
            "NEV 未匹配到电池续航字段，请确认表头为 Battery range。"
        )
        return
    if nev_df.empty:
        if nev_pt_rows > 0:
            st.warning(
                "已找到 BEV/PHEV 记录，但 Battery range 在当前筛选下均为空或非数值。"
            )
            return
        show_no_data("NEV续航分布")
        return

    growth_mode_label = "时间窗净变化（末年-首年）"
    year_column_map = group_selected_columns_by_year(
        selected_columns,
        selection.grain,
    )
    year_labels = list(year_column_map.keys())
    start_year_label = year_labels[0] if year_labels else None
    end_year_label = year_labels[-1] if year_labels else None

    sales_year_columns: dict[str, str] = {}
    for year_label, year_columns in year_column_map.items():
        sales_series = sum_sales_for_columns(filtered_df, year_columns)
        sales_col_name = f"SalesYear_{year_label}"
        nev_df[sales_col_name] = pd.to_numeric(
            sales_series,
            errors="coerce",
        ).fillna(0.0)
        sales_year_columns[year_label] = sales_col_name

    start_sales_col = (
        sales_year_columns.get(start_year_label)
        if start_year_label
        else None
    )
    end_sales_col = (
        sales_year_columns.get(end_year_label)
        if end_year_label
        else None
    )

    if start_sales_col:
        nev_df["SalesWindowStartYear"] = nev_df[start_sales_col]
    else:
        nev_df["SalesWindowStartYear"] = 0.0

    if end_sales_col:
        nev_df["SalesWindowEndYear"] = nev_df[end_sales_col]
    else:
        nev_df["SalesWindowEndYear"] = 0.0

    nev_df["GrowthWindow"] = (
        nev_df["SalesWindowEndYear"] - nev_df["SalesWindowStartYear"]
    )
    nev_df["GrowthAbsWindow"] = nev_df["GrowthWindow"].abs()

    growth_ready = bool(
        start_sales_col
        and end_sales_col
        and start_year_label
        and end_year_label
        and start_year_label != end_year_label
    )

    available_pt = set(nev_df["Powertrain"].astype(str))
    nev_options = [
        powertrain
        for powertrain in ["PHEV", "BEV"]
        if powertrain in available_pt
    ]
    if not nev_options:
        st.info("当前筛选下无 BEV/PHEV 数据。")
        return

    reset_col_1, reset_col_2 = st.columns([1, 2])
    with reset_col_1:
        reset_controls = st.button(
            "重置参数",
            key="adv_nev_range_reset_controls",
            width="stretch",
        )
    with reset_col_2:
        st.caption("重置到默认：当前时间窗 + BEV/PHEV + TopN80。")

    if reset_controls:
        st.session_state["adv_nev_range_powertrain"] = list(nev_options)
        st.session_state["adv_nev_range_metric_mode"] = "当前时间窗销量"
        st.session_state["adv_nev_range_topn_enabled"] = True
        st.session_state["adv_nev_range_topn"] = 80
        st.session_state["adv_nev_range_axis_max"] = 1000
        st.session_state["adv_nev_range_axis_step"] = 50
        st.session_state["adv_nev_range_stack_by_model"] = False
        st.session_state["adv_nev_range_facet_brand"] = False
        st.session_state["adv_nev_range_max_brand"] = 4

    legacy_metric_mode = st.session_state.get("adv_nev_range_metric_mode")
    if legacy_metric_mode in {
        "23-25增长变化",
        "23-25净变化（2025-2023）",
    }:
        st.session_state["adv_nev_range_metric_mode"] = growth_mode_label

    control_col_1, control_col_2, control_col_3 = st.columns([1, 1, 1])
    with control_col_1:
        selected_powertrains = st.multiselect(
            "动总类型",
            options=nev_options,
            default=nev_options,
            key="adv_nev_range_powertrain",
        )
    with control_col_2:
        top_n_enabled = st.checkbox(
            "启用 TopN（按当前口径）",
            value=True,
            key="adv_nev_range_topn_enabled",
        )
    with control_col_3:
        top_n = int(
            st.slider(
                "TopN",
                min_value=10,
                max_value=300,
                value=80,
                step=5,
                key="adv_nev_range_topn",
                disabled=not top_n_enabled,
            )
        )

    adv_col_1, adv_col_2, adv_col_3 = st.columns([1, 1, 1])
    with adv_col_1:
        axis_max = int(
            st.slider(
                "续航轴上限",
                min_value=200,
                max_value=1500,
                value=1000,
                step=50,
                key="adv_nev_range_axis_max",
            )
        )
    with adv_col_2:
        range_step = int(
            st.slider(
                "续航分箱步长",
                min_value=10,
                max_value=200,
                value=50,
                step=10,
                key="adv_nev_range_axis_step",
            )
        )
    with adv_col_3:
        metric_mode = st.radio(
            "分布口径",
            ["当前时间窗销量", growth_mode_label],
            horizontal=True,
            key="adv_nev_range_metric_mode",
        )

    split_by_brand = False
    stack_by_model = False
    max_brand_facets = 4
    with st.expander("高级设置", expanded=False):
        stack_by_model = st.checkbox(
            "按Model堆叠显示",
            value=False,
            key="adv_nev_range_stack_by_model",
            disabled=not bool(columns.model),
        )
        split_by_brand = st.checkbox(
            "按品牌分面",
            value=False,
            key="adv_nev_range_facet_brand",
            disabled=not bool(columns.make),
        )
        if split_by_brand:
            max_brand_facets = int(
                st.slider(
                    "最多展示品牌数",
                    min_value=2,
                    max_value=12,
                    value=4,
                    step=1,
                    key="adv_nev_range_max_brand",
                )
            )

    if not selected_powertrains:
        st.info("请至少选择一个动总类型。")
        return

    if metric_mode == growth_mode_label and not growth_ready:
        st.warning(
            "当前时间窗不足两个年份，已回退到当前时间窗销量。"
        )
        metric_mode = "当前时间窗销量"

    metric_column = "SalesWindow"
    metric_title = "销量"
    ranking_column = "SalesWindow"
    if metric_mode == growth_mode_label:
        metric_column = "GrowthWindow"
        if growth_ready and start_year_label and end_year_label:
            metric_title = f"销量净变化（{end_year_label}-{start_year_label}）"
        else:
            metric_title = "销量净变化（末年-首年）"
        ranking_column = "GrowthAbsWindow"

    nev_df = nev_df[nev_df["Powertrain"].isin(selected_powertrains)]
    nev_df = nev_df[nev_df["BatteryRange"].between(0, axis_max)]
    if nev_df.empty:
        if nev_pt_rows > 0:
            st.warning(
                "当前筛选下 BEV/PHEV 的 Battery range 不在有效区间内，请放宽筛选或调整续航轴上限。"
            )
            return
        show_no_data("NEV续航分布")
        return

    if top_n_enabled:
        model_rank = nev_df.groupby("Model", as_index=False)[
            ranking_column
        ].sum()
        model_rank = model_rank.sort_values(
            ranking_column,
            ascending=False,
        )
        top_models = set(model_rank.head(top_n)["Model"])
        nev_df = nev_df[nev_df["Model"].isin(top_models)]

    if split_by_brand:
        brand_rank = nev_df.groupby("Brand", as_index=False)[
            ranking_column
        ].sum()
        brand_rank = brand_rank.sort_values(
            ranking_column,
            ascending=False,
        )
        selected_brands = brand_rank.head(max_brand_facets)["Brand"].tolist()
        nev_df = nev_df[nev_df["Brand"].isin(selected_brands)]
    else:
        selected_brands = []

    if nev_df.empty:
        show_no_data("NEV续航分布")
        return

    net_change_total = 0.0
    abs_change_total = 0.0
    offset_ratio = 0.0
    weighted_range_start: float | None = None
    weighted_range_end: float | None = None
    powertrain_tokens: list[str] = []
    bucket_summary_df = pd.DataFrame()
    bucket_positive_df = pd.DataFrame()
    bucket_negative_df = pd.DataFrame()
    model_mover_df = pd.DataFrame()
    model_gain_df = pd.DataFrame()
    model_decline_df = pd.DataFrame()
    top_model_limit = 0
    top_model_abs_share: float | None = None
    topn_abs_share_alert_threshold = 0.70

    if metric_mode == growth_mode_label:
        net_change_total = float(nev_df["GrowthWindow"].sum())
        abs_change_total = float(nev_df["GrowthAbsWindow"].sum())
        if abs_change_total > 0:
            offset_ratio = 1.0 - abs(net_change_total) / abs_change_total

        def _weighted_avg_range(sales_col: str) -> float | None:
            weight_sum = float(nev_df[sales_col].sum())
            if weight_sum <= 0:
                return None
            weighted_sum = float(
                (nev_df["BatteryRange"] * nev_df[sales_col]).sum()
            )
            return weighted_sum / weight_sum

        weighted_range_start = _weighted_avg_range("SalesWindowStartYear")
        weighted_range_end = _weighted_avg_range("SalesWindowEndYear")

        powertrain_summary = (
            nev_df.groupby("Powertrain", as_index=False)[
                ["GrowthWindow", "GrowthAbsWindow"]
            ]
            .sum()
            .sort_values("GrowthAbsWindow", ascending=False)
        )
        for _, row in powertrain_summary.iterrows():
            share_text = (
                f"{row['GrowthWindow'] / net_change_total:.1%}"
                if net_change_total != 0
                else "N/A"
            )
            powertrain_tokens.append(
                f"{row['Powertrain']} {row['GrowthWindow']:,.0f} "
                f"({share_text})"
            )

        bucket_labels = ["0-399", "400-499", "500-599", "600-1000"]
        bucket_source = nev_df.copy()
        bucket_source["RangeBucket"] = pd.cut(
            bucket_source["BatteryRange"],
            bins=[-1, 399, 499, 599, 1000],
            labels=bucket_labels,
        )
        bucket_summary_df = (
            bucket_source.groupby(
                "RangeBucket",
                as_index=False,
                observed=False,
            )[[
                "SalesWindowStartYear",
                "SalesWindowEndYear",
                "GrowthWindow",
            ]]
            .sum()
        )
        if net_change_total != 0:
            bucket_summary_df["NetShare"] = (
                bucket_summary_df["GrowthWindow"] / net_change_total
            )
        else:
            bucket_summary_df["NetShare"] = 0.0

        if not bucket_summary_df.empty:
            bucket_positive_df = (
                bucket_summary_df.sort_values(
                    "GrowthWindow",
                    ascending=False,
                )
                .head(3)
                .copy()
            )
            bucket_negative_df = (
                bucket_summary_df.sort_values(
                    "GrowthWindow",
                    ascending=True,
                )
                .head(3)
                .copy()
            )

        model_mover_df = (
            nev_df.groupby("Model", as_index=False)[
                [
                    "GrowthWindow",
                    "GrowthAbsWindow",
                    "SalesWindowStartYear",
                    "SalesWindowEndYear",
                ]
            ]
            .sum()
            .sort_values("GrowthAbsWindow", ascending=False)
        )
        top_model_limit = int(min(10, len(model_mover_df)))
        if top_model_limit > 0 and abs_change_total > 0:
            top_model_abs_share = float(
                model_mover_df.head(top_model_limit)["GrowthAbsWindow"].sum()
                / abs_change_total
            )

        if top_model_limit > 0:
            model_gain_df = (
                model_mover_df.sort_values(
                    "GrowthWindow",
                    ascending=False,
                )
                .head(top_model_limit)
                .copy()
            )
            model_decline_df = (
                model_mover_df.sort_values(
                    "GrowthWindow",
                    ascending=True,
                )
                .head(top_model_limit)
                .copy()
            )

    nev_df["RangeBandStart"] = (
        (pd.to_numeric(nev_df["BatteryRange"], errors="coerce") // range_step)
        * range_step
    )
    nev_df["RangeBandStart"] = nev_df["RangeBandStart"].clip(
        lower=0,
        upper=max(0, axis_max - range_step),
    )

    color_dimension = "Model" if stack_by_model else "Powertrain"
    group_fields = ["RangeBandStart", color_dimension]
    if split_by_brand:
        group_fields.insert(0, "Brand")

    plot_df = nev_df.groupby(group_fields, as_index=False)[metric_column].sum()
    plot_df = plot_df.sort_values(group_fields)
    if plot_df.empty:
        show_no_data("NEV续航分布")
        return

    plot_kwargs: dict[str, Any] = {"orientation": "h"}
    if not stack_by_model:
        plot_kwargs["color_discrete_map"] = {
            key: value
            for key, value in POWERTRAIN_COLOR_MAP.items()
            if key in selected_powertrains
        }
    if split_by_brand:
        plot_kwargs["facet_col"] = "Brand"
        plot_kwargs["facet_col_wrap"] = 3
        plot_kwargs["category_orders"] = {"Brand": selected_brands}

    if metric_mode == growth_mode_label:
        if growth_ready and start_year_label and end_year_label:
            chart_title = (
                f"NEV 续航分布变化（{end_year_label}-{start_year_label}）"
            )
        else:
            chart_title = "NEV 续航分布变化（末年-首年）"
    else:
        chart_title = (
            "NEV 续航分布（Model堆叠）"
            if stack_by_model
            else "NEV 续航分布（BEV/PHEV）"
        )

    fig = px.bar(
        plot_df,
        x=metric_column,
        y="RangeBandStart",
        color=color_dimension,
        title=chart_title,
        **plot_kwargs,
    )
    fig.update_layout(barmode="stack")
    if split_by_brand:
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace("Brand=", "")
            )
        )

    fig = style_figure(fig)
    fig.update_yaxes(
        title=f"Battery range（{range_col}）",
        range=[0, axis_max],
        dtick=range_step,
    )
    fig.update_xaxes(title=metric_title)
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_nev_range_distribution",
        filename_prefix="nev_range_distribution",
    )
    if metric_mode == growth_mode_label:
        if growth_ready and start_year_label and end_year_label:
            growth_span_label = f"{end_year_label}-{start_year_label}"
        else:
            growth_span_label = "末年-首年"

        annual_sales_tokens: list[str] = []
        for year_label in year_labels:
            sales_col_name = sales_year_columns.get(year_label)
            if not sales_col_name:
                continue
            annual_sales_tokens.append(
                f"{year_label} {float(nev_df[sales_col_name].sum()):,.0f}"
            )
        if annual_sales_tokens:
            st.caption("NEV 年度销量：" + "｜".join(annual_sales_tokens))
        (
            insight_col_1,
            insight_col_2,
            insight_col_3,
            insight_col_4,
        ) = st.columns(4)
        with insight_col_1:
            st.metric("时间窗净变化", f"{net_change_total:,.0f}")
        with insight_col_2:
            st.metric("|净变化|总量", f"{abs_change_total:,.0f}")
        with insight_col_3:
            st.metric("结构对冲率", f"{offset_ratio:.1%}")
        with insight_col_4:
            weighted_end_text = (
                f"{weighted_range_end:,.1f} km"
                if weighted_range_end is not None
                else "N/A"
            )
            weighted_delta_text: str | None = None
            if (
                weighted_range_start is not None
                and weighted_range_end is not None
            ):
                if start_year_label:
                    weighted_delta_text = (
                        f"{weighted_range_end - weighted_range_start:+.1f} km "
                        f"vs {start_year_label}"
                    )
                else:
                    weighted_delta_text = (
                        f"{weighted_range_end - weighted_range_start:+.1f} km"
                    )
            st.metric(
                "销量加权平均续航(末年)",
                weighted_end_text,
                delta=weighted_delta_text,
            )

        if powertrain_tokens:
            st.caption("净变化贡献：" + "｜".join(powertrain_tokens))
        if top_model_abs_share is not None:
            st.caption(
                f"Top{top_model_limit} Model 贡献了 "
                f"{top_model_abs_share:.1%} 的 |净变化|。"
            )
            if top_model_abs_share >= topn_abs_share_alert_threshold:
                st.warning(
                    f"Top{top_model_limit} |净变化|集中度 "
                    f"{top_model_abs_share:.1%} >= "
                    f"{topn_abs_share_alert_threshold:.0%}，"
                    "结构风险较高，建议关注头部车型波动。"
                )
        if offset_ratio >= 0.85:
            st.info(
                "对冲率较高：净增背后存在较强的车型结构迁移，建议结合分桶与 Top 车型明细一起看。"
            )

        with st.expander("查看净变化结构拆解", expanded=False):
            if not bucket_summary_df.empty:
                bucket_display = bucket_summary_df.rename(
                    columns={
                        "RangeBucket": "续航分桶(km)",
                        "SalesWindowStartYear": (
                            f"{start_year_label or '首年'}销量"
                        ),
                        "SalesWindowEndYear": (
                            f"{end_year_label or '末年'}销量"
                        ),
                        "GrowthWindow": f"净变化({growth_span_label})",
                        "NetShare": "净变化贡献",
                    }
                ).copy()
                bucket_display["净变化贡献"] = bucket_display["净变化贡献"].map(
                    lambda value: f"{value:.1%}"
                )
                st.dataframe(
                    bucket_display,
                    width="stretch",
                    hide_index=True,
                )

            if not bucket_positive_df.empty and not bucket_negative_df.empty:
                bucket_pos_col, bucket_neg_col = st.columns(2)
                with bucket_pos_col:
                    st.caption("续航分桶净变化 Top 正向")
                    bucket_pos_display = bucket_positive_df.rename(
                        columns={
                            "RangeBucket": "续航分桶(km)",
                            "GrowthWindow": (
                                f"净变化({growth_span_label})"
                            ),
                            "NetShare": "净变化贡献",
                        }
                    ).copy()
                    bucket_pos_display["净变化贡献"] = (
                        bucket_pos_display["净变化贡献"].map(
                            lambda value: f"{value:.1%}"
                        )
                    )
                    st.dataframe(
                        bucket_pos_display[
                            [
                                "续航分桶(km)",
                                f"净变化({growth_span_label})",
                                "净变化贡献",
                            ]
                        ],
                        width="stretch",
                        hide_index=True,
                    )
                with bucket_neg_col:
                    st.caption("续航分桶净变化 Top 负向")
                    bucket_neg_display = bucket_negative_df.rename(
                        columns={
                            "RangeBucket": "续航分桶(km)",
                            "GrowthWindow": (
                                f"净变化({growth_span_label})"
                            ),
                            "NetShare": "净变化贡献",
                        }
                    ).copy()
                    bucket_neg_display["净变化贡献"] = (
                        bucket_neg_display["净变化贡献"].map(
                            lambda value: f"{value:.1%}"
                        )
                    )
                    st.dataframe(
                        bucket_neg_display[
                            [
                                "续航分桶(km)",
                                f"净变化({growth_span_label})",
                                "净变化贡献",
                            ]
                        ],
                        width="stretch",
                        hide_index=True,
                    )

            if not model_mover_df.empty:
                st.caption("Top 车型（按 |净变化| 排序）")
                model_display = (
                    model_mover_df.head(top_model_limit)
                    .rename(
                        columns={
                            "Model": "Model",
                            "GrowthWindow": f"净变化({growth_span_label})",
                            "GrowthAbsWindow": "|净变化|",
                            "SalesWindowStartYear": (
                                f"{start_year_label or '首年'}销量"
                            ),
                            "SalesWindowEndYear": (
                                f"{end_year_label or '末年'}销量"
                            ),
                        }
                    )
                    .copy()
                )
                st.dataframe(
                    model_display,
                    width="stretch",
                    hide_index=True,
                )

            if not model_gain_df.empty and not model_decline_df.empty:
                gain_col, decline_col = st.columns(2)
                with gain_col:
                    st.caption(f"Top{top_model_limit} 正向车型")
                    gain_display = model_gain_df.rename(
                        columns={
                            "Model": "Model",
                            "GrowthWindow": f"净变化({growth_span_label})",
                            "SalesWindowStartYear": (
                                f"{start_year_label or '首年'}销量"
                            ),
                            "SalesWindowEndYear": (
                                f"{end_year_label or '末年'}销量"
                            ),
                        }
                    ).copy()
                    st.dataframe(
                        gain_display[
                            [
                                "Model",
                                f"净变化({growth_span_label})",
                                f"{start_year_label or '首年'}销量",
                                f"{end_year_label or '末年'}销量",
                            ]
                        ],
                        width="stretch",
                        hide_index=True,
                    )
                with decline_col:
                    st.caption(f"Top{top_model_limit} 负向车型")
                    decline_display = model_decline_df.rename(
                        columns={
                            "Model": "Model",
                            "GrowthWindow": f"净变化({growth_span_label})",
                            "SalesWindowStartYear": (
                                f"{start_year_label or '首年'}销量"
                            ),
                            "SalesWindowEndYear": (
                                f"{end_year_label or '末年'}销量"
                            ),
                        }
                    ).copy()
                    st.dataframe(
                        decline_display[
                            [
                                "Model",
                                f"净变化({growth_span_label})",
                                f"{start_year_label or '首年'}销量",
                                f"{end_year_label or '末年'}销量",
                            ]
                        ],
                        width="stretch",
                        hide_index=True,
                    )

        st.caption(
            f"当前口径：图值={growth_span_label} 净变化；"
            "TopN 按 |净变化| 排序。"
        )
    else:
        st.caption(f"当前口径：{selection.start_label}~{selection.end_label} 销量。")
    stack_caption = "Model" if stack_by_model else "Powertrain"
    st.caption(f"当前堆叠维度：{stack_caption}")


def render_chart_nev_capacity_vs_msrp(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, selected_columns = get_time_selection_for_chart(
        chart_name="NEV电池容量与MSRP",
        key_prefix="adv_nev_capacity_msrp",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return
    if not columns.powertrain:
        st.warning("缺少动总字段，无法绘制电池容量与 MSRP 关系图。")
        return

    capacity_col, capacity_values = prepare_numeric_axis(
        filtered_df,
        list(BATTERY_CAPACITY_CANDIDATES),
    )
    msrp_col, msrp_values = prepare_numeric_axis(
        filtered_df,
        list(MSRP_CANDIDATES),
    )
    if not capacity_col:
        st.warning(
            "NEV 未匹配到电池容量字段，请确认表头为 Battery kwh。"
        )
        return
    if not msrp_col:
        st.warning("NEV 未匹配到 MSRP 字段，无法绘制关系图。")
        return

    sales_ref = sum_sales_for_columns(filtered_df, selected_columns)
    model_series = (
        normalize_series(filtered_df[columns.model])
        if columns.model
        else pd.Series("未标注", index=filtered_df.index)
    )
    brand_series = (
        normalize_series(filtered_df[columns.make])
        if columns.make
        else pd.Series("全部品牌", index=filtered_df.index)
    )
    nev_df = pd.DataFrame(
        {
            "Model": model_series,
            "Brand": brand_series,
            "Powertrain": normalize_powertrain_for_nev(
                filtered_df[columns.powertrain]
            ),
            "BatteryCapacity": capacity_values,
            "MSRP": msrp_values,
            "Sales": pd.to_numeric(sales_ref, errors="coerce").fillna(0.0),
        }
    )

    nev_pt_rows = int(
        nev_df["Powertrain"].isin(["BEV", "PHEV"]).sum()
    )

    nev_df = nev_df.dropna(subset=["BatteryCapacity", "MSRP"])
    nev_df = nev_df[
        (nev_df["BatteryCapacity"] > 0)
        & (nev_df["MSRP"] > 0)
    ]
    if nev_df.empty:
        if nev_pt_rows > 0:
            st.warning(
                "已找到 BEV/PHEV 记录，但 Battery kwh 或 MSRP 在当前筛选下为空或非正值。"
            )
            return
        show_no_data("NEV电池容量与MSRP")
        return

    available_pt = set(nev_df["Powertrain"].astype(str))
    nev_options = [
        powertrain
        for powertrain in ["PHEV", "BEV"]
        if powertrain in available_pt
    ]
    if not nev_options:
        st.info("当前筛选下无 BEV/PHEV 数据。")
        return

    reset_col_1, reset_col_2 = st.columns([1, 2])
    with reset_col_1:
        reset_controls = st.button(
            "重置参数",
            key="adv_nev_capacity_reset_controls",
            width="stretch",
        )
    with reset_col_2:
        st.caption("重置到默认：BEV/PHEV + TopN120。")

    if reset_controls:
        st.session_state["adv_nev_capacity_powertrain"] = list(nev_options)
        st.session_state["adv_nev_capacity_topn"] = 120
        st.session_state["adv_nev_capacity_split_brand"] = False

    top_col_1, top_col_2, top_col_3 = st.columns([1, 1, 1])
    with top_col_1:
        selected_powertrains = st.multiselect(
            "动总类型",
            options=nev_options,
            default=nev_options,
            key="adv_nev_capacity_powertrain",
        )
    with top_col_2:
        top_n = int(
            st.slider(
                "TopN（按销量）",
                min_value=20,
                max_value=300,
                value=120,
                step=10,
                key="adv_nev_capacity_topn",
            )
        )
    with top_col_3:
        split_by_brand = st.checkbox(
            "按品牌分面",
            value=False,
            key="adv_nev_capacity_split_brand",
            disabled=not bool(columns.make),
        )

    if not selected_powertrains:
        st.info("请至少选择一个动总类型。")
        return

    nev_df = nev_df[nev_df["Powertrain"].isin(selected_powertrains)]
    if nev_df.empty:
        show_no_data("NEV电池容量与MSRP")
        return

    model_df = nev_df.groupby(
        ["Model", "Brand", "Powertrain"],
        as_index=False,
    ).agg(
        BatteryCapacity=("BatteryCapacity", "median"),
        MSRP=("MSRP", "median"),
        Sales=("Sales", "sum"),
    )
    model_df = model_df.sort_values("Sales", ascending=False).head(top_n)
    if model_df.empty:
        show_no_data("NEV电池容量与MSRP")
        return

    scatter_kwargs: dict[str, Any] = {
        "size_max": 45,
        "color_discrete_map": {
            key: value
            for key, value in POWERTRAIN_COLOR_MAP.items()
            if key in selected_powertrains
        },
    }
    if split_by_brand:
        top_brands = (
            model_df.groupby("Brand", as_index=False)["Sales"]
            .sum()
            .sort_values("Sales", ascending=False)
            .head(6)["Brand"]
            .tolist()
        )
        model_df = model_df[model_df["Brand"].isin(top_brands)]
        scatter_kwargs["facet_col"] = "Brand"
        scatter_kwargs["facet_col_wrap"] = 3
        scatter_kwargs["category_orders"] = {"Brand": top_brands}

    fig = px.scatter(
        model_df,
        x="BatteryCapacity",
        y="MSRP",
        size="Sales",
        color="Powertrain",
        hover_name="Model",
        hover_data={
            "Brand": True,
            "BatteryCapacity": ":,.1f",
            "MSRP": ":,.0f",
            "Sales": ":,.0f",
        },
        title="NEV Battery Capacity vs MSRP",
        **scatter_kwargs,
    )
    if split_by_brand:
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace("Brand=", "")
            )
        )
    fig = style_figure(fig)
    fig.update_xaxes(title=f"Battery kwh（{capacity_col}）")
    fig.update_yaxes(title=f"MSRP（{msrp_col}）")
    render_plotly_chart_with_png_export(
        fig=fig,
        chart_key="adv_nev_capacity_msrp",
        filename_prefix="nev_capacity_msrp",
    )

    corr_value = model_df["BatteryCapacity"].corr(model_df["MSRP"])
    if pd.notna(corr_value):
        st.caption(
            f"容量-价格相关系数（TopN）：{float(corr_value):.3f}"
        )
    st.caption(
        f"当前口径：{selection.start_label}~{selection.end_label} 销量。"
    )


def render_chart_seasonality_heatmap(
    filtered_df: pd.DataFrame,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    selection, month_columns = get_time_selection_for_chart(
        chart_name="季节性热力图",
        key_prefix="heatmap_chart",
        time_axis=time_axis,
        global_time_selection=global_time_selection,
    )
    if not selection:
        return

    if selection.grain != "month":
        st.warning("当前为年度时间轴，无法绘制月度季节性热力图。")
        return

    month_total = (
        filtered_df[month_columns]
        .sum()
        .rename_axis("Month")
        .reset_index(name="Sales")
    )
    month_total["Date"] = pd.to_datetime(
        month_total["Month"],
        format="%Y %b",
        errors="coerce",
    )
    month_total = month_total.dropna(subset=["Date"])

    if month_total.empty:
        show_no_data("季节性热力图")
        return

    month_total["Year"] = month_total["Date"].dt.year.astype(str)
    month_total["MonthLabel"] = month_total["Date"].dt.strftime("%b")

    month_order = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    heatmap_df = month_total.pivot_table(
        index="Year",
        columns="MonthLabel",
        values="Sales",
        aggfunc="sum",
        fill_value=0,
    )
    heatmap_df = heatmap_df.reindex(columns=month_order, fill_value=0)
    heatmap_df = heatmap_df.sort_index()

    fig_heatmap = px.imshow(
        heatmap_df,
        aspect="auto",
        labels={
            "x": "月份",
            "y": "年份",
            "color": "销量",
        },
        title="月度季节性热力图（总和）",
        color_continuous_scale="Blues",
    )
    fig_heatmap.update_layout(template="plotly_white")
    render_plotly_chart_with_png_export(
        fig=fig_heatmap,
        chart_key="adv_seasonality_heatmap",
        filename_prefix="seasonality_heatmap",
    )


def render_advanced_charts(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    with st.container(border=True):
        st.subheader("增强分析图")
        chart_groups: dict[str, dict[str, object]] = {
            "market_structure": {
                "label": "市场结构",
                "charts": [
                    "powertrain_bubble",
                    "seasonality_heatmap",
                    "segment_share_length",
                ],
            },
            "nev_analysis": {
                "label": "NEV分析",
                "charts": [
                    "nev_range_distribution",
                    "nev_capacity_msrp",
                ],
            },
            "price_value": {
                "label": "价格价值",
                "charts": [
                    "price_migration",
                    "length_price_map",
                    "price_per_meter_sales",
                    "sales_price_scatter",
                ],
            },
            "powertrain_cost": {
                "label": "动力成本",
                "charts": [
                    "rv_finance_dashboard",
                    "estimated_tco_msrp",
                    "powertrain_price_mix",
                ],
            },
        }

        chart_registry: dict[str, dict[str, object]] = {
            "powertrain_bubble": {
                "label": "动总分布气泡图",
                "help": "看车型在车长-价格平面上的动总分布与销量权重。",
                "render": lambda: render_chart_powertrain_bubble(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "seasonality_heatmap": {
                "label": "季节性热力图",
                "help": "看月度销量季节性与年度内波动强弱。",
                "render": lambda: render_chart_seasonality_heatmap(
                    filtered_df,
                    time_axis,
                    global_time_selection,
                ),
            },
            "nev_range_distribution": {
                "label": "NEV续航分布",
                "help": "BEV/PHEV 续航分布（支持TopN、23-25增长口径与品牌分面）。",
                "render": lambda: render_chart_nev_range_distribution(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "nev_capacity_msrp": {
                "label": "容量 vs MSRP",
                "help": "查看 BEV/PHEV 电池容量与 MSRP 的关系。",
                "render": lambda: render_chart_nev_capacity_vs_msrp(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "price_migration": {
                "label": "价格带迁移图",
                "help": "看不同年份在各价格带的销量迁移与峰值变化。",
                "render": lambda: render_chart_price_migration(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "length_price_map": {
                "label": "尺寸—价格地图",
                "help": "看车型尺寸与价格定位关系，识别潜在越级价值点。",
                "render": lambda: render_chart_length_vs_price_map(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "price_per_meter_sales": {
                "label": "单位尺寸价格 vs 销量",
                "help": "看单位车长价格密度与销量表现的关系。",
                "render": lambda: render_chart_price_per_meter_vs_sales(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "powertrain_price_mix": {
                "label": "动力结构 vs 价格",
                "help": "看不同价格带的动力类型结构占比。",
                "render": lambda: render_chart_powertrain_vs_price(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "sales_price_scatter": {
                "label": "销量—价格散点",
                "help": "看车型价格与销量分布，比较细分市场份额。",
                "render": lambda: render_chart_sales_vs_price_scatter(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "segment_share_length": {
                "label": "尺寸段份额",
                "help": "看不同车长分段下的细分市场份额结构。",
                "render": lambda: render_chart_segment_share_by_length(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "estimated_tco_msrp": {
                "label": "估算TCO vs MSRP",
                "help": "在可调参数下看估算TCO与MSRP的相对关系。",
                "render": lambda: render_chart_estimated_tco_vs_msrp(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
            "rv_finance_dashboard": {
                "label": "RV金融杠杆",
                "help": "输入首付、残值、APR 与期限，查看净融资额与月供推导。",
                "render": lambda: render_chart_rv_finance_dashboard(
                    filtered_df,
                    columns,
                    time_axis,
                    global_time_selection,
                ),
            },
        }

        group_options = list(chart_groups.keys())
        group_state_key = "advanced_charts_group"
        chart_state_key = "advanced_charts_chart"

        default_group = "market_structure"
        if default_group not in chart_groups:
            default_group = group_options[0]

        default_chart = "powertrain_bubble"
        group_chart_memory_key = "advanced_charts_group_chart_memory"

        if (
            group_chart_memory_key not in st.session_state
            or not isinstance(st.session_state[group_chart_memory_key], dict)
        ):
            st.session_state[group_chart_memory_key] = {}

        if (
            group_state_key not in st.session_state
            or st.session_state[group_state_key] not in group_options
        ):
            st.session_state[group_state_key] = default_group

        current_group = str(st.session_state[group_state_key])
        current_chart_options = [
            chart_id
            for chart_id in chart_groups[current_group]["charts"]
            if chart_id in chart_registry
        ]
        remembered_for_group = st.session_state[group_chart_memory_key].get(
            current_group
        )
        if (
            chart_state_key not in st.session_state
            or st.session_state[chart_state_key] not in current_chart_options
        ):
            if remembered_for_group in current_chart_options:
                st.session_state[chart_state_key] = remembered_for_group
            elif (
                current_group == default_group
                and default_chart in current_chart_options
            ):
                st.session_state[chart_state_key] = default_chart
            elif current_chart_options:
                st.session_state[chart_state_key] = current_chart_options[0]

        def select_group(target_group: str) -> None:
            st.session_state[group_state_key] = target_group
            target_chart_options = [
                chart_id
                for chart_id in chart_groups[target_group]["charts"]
                if chart_id in chart_registry
            ]
            remembered = st.session_state[group_chart_memory_key].get(
                target_group
            )
            if remembered in target_chart_options:
                st.session_state[chart_state_key] = remembered
            elif (
                target_group == default_group
                and default_chart in target_chart_options
            ):
                st.session_state[chart_state_key] = default_chart
            elif target_chart_options:
                st.session_state[chart_state_key] = target_chart_options[0]

        def select_chart(target_chart: str) -> None:
            st.session_state[chart_state_key] = target_chart
            active_group = str(
                st.session_state.get(group_state_key, default_group)
            )
            memory = st.session_state.get(group_chart_memory_key, {})
            if not isinstance(memory, dict):
                memory = {}
            memory[active_group] = target_chart
            st.session_state[group_chart_memory_key] = memory

        st.caption("悬停业务组按钮可查看该组子图清单")
        group_cols = st.columns(len(group_options))
        for idx, group_key in enumerate(group_options):
            group_label = str(chart_groups[group_key]["label"])
            group_chart_labels = [
                str(chart_registry[chart_id]["label"])
                for chart_id in chart_groups[group_key]["charts"]
                if chart_id in chart_registry
            ]
            group_tooltip = "子图：\n- " + "\n- ".join(group_chart_labels)
            with group_cols[idx]:
                st.button(
                    group_label,
                    key=f"{group_state_key}_btn_{group_key}",
                    help=group_tooltip,
                    type=(
                        "primary"
                        if st.session_state[group_state_key] == group_key
                        else "secondary"
                    ),
                    on_click=select_group,
                    args=(group_key,),
                    width="stretch",
                )

        selected_group = str(st.session_state[group_state_key])

        chart_options = [
            chart_id
            for chart_id in chart_groups[selected_group]["charts"]
            if chart_id in chart_registry
        ]
        if not chart_options:
            st.info("当前业务组暂未配置图表。")
            return

        if (
            chart_state_key not in st.session_state
            or st.session_state[chart_state_key] not in chart_options
        ):
            st.session_state[chart_state_key] = chart_options[0]

        st.caption("悬停分析图按钮可查看图表用途")
        chart_cols = st.columns(len(chart_options))
        for idx, chart_id in enumerate(chart_options):
            chart_label = str(chart_registry[chart_id]["label"])
            chart_help = str(chart_registry[chart_id].get("help", ""))
            with chart_cols[idx]:
                st.button(
                    chart_label,
                    key=f"{chart_state_key}_btn_{chart_id}",
                    help=chart_help,
                    type=(
                        "primary"
                        if st.session_state[chart_state_key] == chart_id
                        else "secondary"
                    ),
                    on_click=select_chart,
                    args=(chart_id,),
                    width="stretch",
                )

        selected_chart = str(st.session_state[chart_state_key])

        memory = st.session_state.get(group_chart_memory_key, {})
        if isinstance(memory, dict):
            memory[selected_group] = selected_chart
            st.session_state[group_chart_memory_key] = memory

        st.caption(
            (
                f"当前路径：{chart_groups[selected_group]['label']}"
                f" / {chart_registry[selected_chart]['label']}"
            )
        )

        render_func = chart_registry[selected_chart]["render"]
        if callable(render_func):
            render_func()


def render_detail_preview(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
) -> None:
    with st.container(border=True):
        with st.expander("🔍 查看明细表（预览）", expanded=False):
            preview_rows = st.slider(
                "预览行数",
                min_value=100,
                max_value=20000,
                value=1000,
                step=100,
            )

            default_cols = [
                col
                for col in [
                    columns.country,
                    columns.segment,
                    columns.powertrain,
                    columns.make,
                    columns.model,
                    columns.version,
                ]
                if col
            ]
            default_cols = dedupe_preserve_order(default_cols)

            if not default_cols:
                default_cols = filtered_df.columns[:12].tolist()

            show_cols = st.multiselect(
                "显示列",
                filtered_df.columns.tolist(),
                default=default_cols,
            )

            ordered_cols = show_cols
            if show_cols:
                st.caption("列顺序支持拖拽：从上到下 = 表格从左到右")
                sortable = get_sort_items_callable()
                if sortable is not None:
                    try:
                        dragged_cols = sortable(
                            show_cols,
                            direction="vertical",
                        )
                        if isinstance(dragged_cols, list):
                            ordered_cols = [
                                column
                                for column in dragged_cols
                                if column in show_cols
                            ]
                            for column in show_cols:
                                if column not in ordered_cols:
                                    ordered_cols.append(column)
                    except Exception:
                        ordered_cols = show_cols
                else:
                    st.info(
                        "当前环境未安装拖拽组件，已使用默认顺序。"
                        "可安装：pip install streamlit-sortables"
                    )

            preview_df = (
                filtered_df[ordered_cols].head(preview_rows)
                if ordered_cols
                else filtered_df.head(preview_rows)
            )

            st.dataframe(preview_df, width="stretch", height=520)
            if len(filtered_df) > preview_rows:
                st.info(
                    f"仅显示前 {preview_rows:,} 行，完整结果共 {len(filtered_df):,} 行。"
                )

            (
                csv_bytes,
                csv_size_bytes,
                is_truncated,
                exceeds_size_limit,
            ) = build_preview_csv_payload(preview_df)

            if is_truncated:
                st.caption(
                    f"下载将截断为前 {CSV_DOWNLOAD_MAX_ROWS:,} 行（安全阈值）。"
                )

            if exceeds_size_limit:
                st.warning(
                    "CSV 文件超过安全阈值，已禁用下载；"
                    "请缩小筛选范围、减少显示列或降低预览行数。"
                )
            else:
                st.download_button(
                    "下载当前预览 CSV",
                    data=csv_bytes,
                    file_name="jato_preview.csv",
                    mime="text/csv",
                    width="content",
                )


def build_preview_csv_payload(
    preview_df: pd.DataFrame,
) -> tuple[bytes, int, bool, bool]:
    download_df = preview_df.head(CSV_DOWNLOAD_MAX_ROWS)
    csv_bytes = download_df.to_csv(index=False).encode("utf-8-sig")
    csv_size_bytes = int(len(csv_bytes))
    is_truncated = bool(len(preview_df) > CSV_DOWNLOAD_MAX_ROWS)
    exceeds_size_limit = bool(csv_size_bytes > CSV_DOWNLOAD_MAX_BYTES)
    return csv_bytes, csv_size_bytes, is_truncated, exceeds_size_limit


def get_default_render_strategy(
    large_data_mode: bool,
    row_count: int,
) -> tuple[bool, bool]:
    lazy_overview_render = bool(large_data_mode and row_count >= 80_000)
    lazy_advanced_render = bool(lazy_overview_render or row_count >= 200_000)
    return lazy_overview_render, lazy_advanced_render


def render_dashboard(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selections: FilterSelections,
    detail_df: pd.DataFrame | None = None,
    large_data_mode: bool = False,
    lazy_overview_render: bool | None = None,
    primary_overview_chart: str | None = None,
    lazy_advanced_render: bool | None = None,
) -> None:
    reset_compute_cache()
    render_timing: dict[str, float] = {}

    stage_start = time.perf_counter()
    render_header_card(filtered_df)
    render_timing["Header"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    time_axis = build_time_axis(filtered_df)
    if time_axis:
        global_time_selection = render_global_time_controls(time_axis)
    else:
        global_time_selection = None
        show_time_axis_unavailable("时间轴功能")
    render_timing["Time Controls"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    render_kpi_cards(
        filtered_df,
        columns,
        time_axis,
        global_time_selection,
    )
    controls = render_line_mode_controls(columns)
    series_order = get_series_order(filtered_df, controls)
    render_top_n_others_detail(filtered_df, controls, series_order)
    render_timing["KPI & Controls"] = time.perf_counter() - stage_start

    (
        default_lazy_render,
        default_lazy_advanced,
    ) = get_default_render_strategy(
        large_data_mode=large_data_mode,
        row_count=len(filtered_df),
    )
    if lazy_overview_render is None:
        lazy_overview_render = default_lazy_render
    if lazy_advanced_render is None:
        lazy_advanced_render = default_lazy_advanced
    if primary_overview_chart not in {"年度趋势", "月度细化"}:
        primary_overview_chart = "年度趋势"

    year_tab, month_tab = st.tabs(["📅 年度趋势", "🌙 月度细化"])
    with year_tab:
        should_render_year = (
            not lazy_overview_render
            or primary_overview_chart == "年度趋势"
            or st.session_state.get("force_render_year_tab", False)
        )
        if should_render_year:
            stage_start = time.perf_counter()
            year_stats = render_year_tab(
                filtered_df,
                controls,
                series_order,
                time_axis,
                global_time_selection,
            )
            render_timing["Year Tab"] = time.perf_counter() - stage_start
            render_timing.update(year_stats)
        else:
            st.info("已按渲染策略延迟年度趋势计算。")
            if st.button(
                "立即渲染年度趋势",
                key="force_render_year_tab_btn",
                width="content",
            ):
                st.session_state["force_render_year_tab"] = True
                st.rerun()
            render_timing["Year Tab"] = 0.0
            render_timing["Year Deferred"] = 0.0

    with month_tab:
        should_render_month = (
            not lazy_overview_render
            or primary_overview_chart == "月度细化"
            or st.session_state.get("force_render_month_tab", False)
        )
        if should_render_month:
            stage_start = time.perf_counter()
            month_stats = render_month_tab(
                filtered_df,
                controls,
                series_order,
                time_axis,
                global_time_selection,
            )
            render_timing["Month Tab"] = time.perf_counter() - stage_start
            render_timing.update(month_stats)
        else:
            st.info("已按渲染策略延迟月度细化计算。")
            if st.button(
                "立即渲染月度细化",
                key="force_render_month_tab_btn",
                width="content",
            ):
                st.session_state["force_render_month_tab"] = True
                st.rerun()
            render_timing["Month Tab"] = 0.0
            render_timing["Month Deferred"] = 0.0

    should_render_advanced = (
        not lazy_advanced_render
        or st.session_state.get("force_render_advanced_charts", False)
    )
    if should_render_advanced:
        stage_start = time.perf_counter()
        render_advanced_charts(
            filtered_df,
            columns,
            time_axis,
            global_time_selection,
        )
        render_timing["Advanced Charts"] = time.perf_counter() - stage_start
    else:
        with st.container(border=True):
            st.subheader("增强分析图")
            st.info("已按渲染策略延迟增强图计算。")
            if st.button(
                "加载增强分析图",
                key="force_render_advanced_charts_btn",
                width="content",
            ):
                st.session_state["force_render_advanced_charts"] = True
                st.rerun()
        render_timing["Advanced Charts"] = 0.0
        render_timing["Advanced Deferred"] = 0.0

    preview_df = detail_df if detail_df is not None else filtered_df
    stage_start = time.perf_counter()
    render_detail_preview(preview_df, columns)
    render_timing["Detail Preview"] = time.perf_counter() - stage_start

    with st.expander("⏱️ 图表渲染耗时（本次）", expanded=False):
        timing_df = pd.DataFrame(
            {
                "模块": list(render_timing.keys()),
                "耗时(s)": [round(value, 3) for value in render_timing.values()],
            }
        ).sort_values("耗时(s)", ascending=False)
        st.dataframe(timing_df, width="stretch", hide_index=True)
