from dataclasses import dataclass

import pandas as pd
import plotly.express as px
import streamlit as st

from .config import APP_TITLE, COLOR_SEQ, PLOT_CONFIG
from .data import dedupe_preserve_order, get_month_columns, get_year_columns
from .models import ColumnRegistry, FilterSelections
from .styles import style_figure


POWERTRAIN_DISPLAY_ORDER = ["BEV", "MHEV", "PHEV", "ICE", "HEV"]
POWERTRAIN_COLOR_MAP = {
    "BEV": "#22C55E",
    "MHEV": "#F59E0B",
    "PHEV": "#3B82F6",
    "ICE": "#9CA3AF",
    "HEV": "#EAB308",
}


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


def build_time_axis(filtered_df: pd.DataFrame) -> TimeAxis | None:
    month_columns = get_month_columns(filtered_df)
    month_entries: list[tuple[pd.Timestamp, str]] = []
    for column in month_columns:
        date_value = pd.to_datetime(
            str(column),
            format="%Y %b",
            errors="coerce",
        )
        if pd.notna(date_value):
            month_entries.append((date_value, str(column)))

    if month_entries:
        month_entries.sort(key=lambda item: item[0])
        dates = tuple(item[0] for item in month_entries)
        labels = tuple(item[1] for item in month_entries)
        return TimeAxis(
            columns=labels,
            labels=labels,
            dates=dates,
            grain="month",
        )

    year_columns = get_year_columns(filtered_df)
    year_entries: list[tuple[pd.Timestamp, str]] = []
    for column in year_columns:
        date_value = pd.to_datetime(
            str(column),
            format="%Y",
            errors="coerce",
        )
        if pd.notna(date_value):
            year_entries.append((date_value, str(column)))

    if not year_entries:
        return None

    year_entries.sort(key=lambda item: item[0])
    dates = tuple(item[0] for item in year_entries)
    labels = tuple(item[1] for item in year_entries)
    return TimeAxis(
        columns=labels,
        labels=labels,
        dates=dates,
        grain="year",
    )


def format_time_selection(selection: TimeSelection) -> str:
    return f"{selection.start_label} ~ {selection.end_label}"


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
        start_idx = labels.index(start_label)
        end_idx = labels.index(end_label)
        selected_indices = list(range(start_idx, end_idx + 1))
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

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        selected_indices = [
            idx
            for idx, value in enumerate(date_values)
            if start_date <= value <= end_date
        ]
        if not selected_indices:
            nearest_idx = min(
                range(len(date_values)),
                key=lambda idx: abs((date_values[idx] - start_date).days),
            )
            selected_indices = [nearest_idx]

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
    text_series = series.astype(str)
    if grain == "month":
        parsed = pd.to_datetime(
            text_series,
            format="%Y %b",
            errors="coerce",
        )
    else:
        parsed = pd.to_datetime(
            text_series,
            format="%Y",
            errors="coerce",
        )

    if parsed.isna().all():
        parsed = pd.to_datetime(text_series, errors="coerce")
    return parsed


def build_time_long_dataframe(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
    grain: str,
    group_column: str | None,
) -> pd.DataFrame:
    if not selected_columns:
        return pd.DataFrame(columns=["Series", "Date", "Sales"])

    if group_column:
        time_df = filtered_df[[group_column] + selected_columns].copy()
        time_df["Series"] = normalize_series(time_df[group_column])
    else:
        time_df = filtered_df[selected_columns].copy()
        time_df["Series"] = "总和"

    long_df = time_df.melt(
        id_vars=["Series"],
        value_vars=selected_columns,
        var_name="TimeKey",
        value_name="Sales",
    )
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

    numeric_df = filtered_df[selected_columns].apply(
        pd.to_numeric,
        errors="coerce",
    )
    return numeric_df.fillna(0.0).sum(axis=1)


def get_sort_items_callable():
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


def add_line_end_labels(fig):
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
    if numeric.notna().sum() > 0:
        return numeric

    extracted = (
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.extract(r"(-?\d+\.?\d*)")[0]
    )
    return pd.to_numeric(extracted, errors="coerce")


def prepare_numeric_axis(
    df: pd.DataFrame,
    candidates: list[str],
) -> tuple[str | None, pd.Series]:
    for candidate in candidates:
        column = find_existing_column(df, [candidate])
        if not column:
            continue

        numeric_series = to_numeric_flexible(df[column])
        if numeric_series.notna().sum() > 0:
            return column, numeric_series

    return None, pd.Series(index=df.index, dtype="float64")


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
) -> None:
    year_columns = get_year_columns(filtered_df)
    total_sales = filtered_df[year_columns].sum().sum() if year_columns else 0

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
        st.metric("累计销量（年度列合计）", f"{total_sales:,.0f}")
    with kpi_col_2:
        st.metric("品牌数", f"{brand_count:,}")
    with kpi_col_3:
        st.metric("Model 数", f"{model_count:,}")
    with kpi_col_4:
        st.metric("Version 数", f"{version_count:,}")


def render_year_tab(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    with st.container(border=True):
        st.subheader("年度对比")

        if not time_axis or not global_time_selection:
            st.warning("未识别可用时间轴，无法绘制年度趋势。")
            return

        time_selection = resolve_chart_time_selection(
            chart_name="年度趋势",
            key_prefix="year_chart",
            time_axis=time_axis,
            global_selection=global_time_selection,
        )
        selected_columns = list(time_selection.columns)
        if not selected_columns:
            st.info("当前时间选择范围内无可用列。")
            return

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
            st.info("当前时间范围下无可展示年度数据。")
            return

        yearly_long["Year"] = yearly_long["Date"].dt.year.astype(str)
        yearly_long = apply_top_n_series(
            yearly_long,
            series_order=series_order,
            include_others=controls.include_others,
        )
        year_plot = yearly_long.groupby(
            ["Series", "Year"],
            as_index=False,
        )["Sales"].sum()
        year_plot["YearSort"] = pd.to_numeric(
            year_plot["Year"],
            errors="coerce",
        )
        year_plot = year_plot.sort_values(["YearSort", "Series"])
        year_plot = year_plot.drop(columns=["YearSort"])

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


def render_month_tab(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> None:
    with st.container(border=True):
        st.subheader("月度细化（支持时间轴调整）")

        if not time_axis or not global_time_selection:
            st.warning("未识别可用时间轴，无法绘制月度细化。")
            return

        time_selection = resolve_chart_time_selection(
            chart_name="月度细化",
            key_prefix="month_chart",
            time_axis=time_axis,
            global_selection=global_time_selection,
        )
        selected_columns = list(time_selection.columns)
        if not selected_columns:
            st.info("当前时间选择范围内无可用列。")
            return

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
            st.info("当前时间范围下无可展示月度数据。")
            return

        period_df = apply_top_n_series(
            period_df,
            series_order=series_order,
            include_others=controls.include_others,
        )
        st.metric("当前时间窗销量总和", f"{period_df['Sales'].sum():,.0f}")

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

        if axis_level == "月":
            period_df["Period"] = period_df["Date"].dt.to_period(
                "M"
            ).dt.to_timestamp()
        elif axis_level == "季度":
            period_df["Period"] = period_df["Date"].dt.to_period(
                "Q"
            ).dt.to_timestamp()
        else:
            period_df["Period"] = period_df["Date"].dt.to_period(
                "Y"
            ).dt.to_timestamp()

        grouped = period_df.groupby(
            ["Series", "Period"],
            as_index=False,
        )["Sales"].sum()
        if grouped.empty:
            st.info("该时间范围内无数据。")
            return

        grouped = grouped.sort_values(["Period", "Series"])
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


def get_time_selection_for_chart(
    chart_name: str,
    key_prefix: str,
    time_axis: TimeAxis | None,
    global_time_selection: TimeSelection | None,
) -> tuple[TimeSelection | None, list[str]]:
    if not time_axis or not global_time_selection:
        st.warning(f"未识别可用时间轴，无法绘制{chart_name}。")
        return None, []

    selection = resolve_chart_time_selection(
        chart_name=chart_name,
        key_prefix=key_prefix,
        time_axis=time_axis,
        global_selection=global_time_selection,
    )
    selected_columns = list(selection.columns)
    if not selected_columns:
        st.info("当前时间选择范围内无可用数据列。")
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
        [
            "MSRP规整",
            "MSRP including delivery charge",
            "MSRP",
            "MSRP区间",
        ],
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
        ["length (mm)", "车长(mm)", "车长", "length"],
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
        [
            "MSRP规整",
            "MSRP including delivery charge",
            "MSRP",
            "MSRP区间",
        ],
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

    invalid_msrp_count = int(
        pd.to_numeric(work_df["MSRP"], errors="coerce")
        .le(0)
        .fillna(False)
        .sum()
    )

    work_df["PriceBand"], band_order = make_price_bands(
        work_df["MSRP"],
        band_size,
    )
    work_df = work_df.dropna(subset=["PriceBand"])
    if work_df.empty:
        st.info("当前筛选下无可用价格带数据。")
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

    st.plotly_chart(
        style_figure(fig),
        width="stretch",
        config=PLOT_CONFIG,
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
        st.info("当前筛选下无可展示尺寸—价格数据。")
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

    st.plotly_chart(
        fig,
        width="stretch",
        config=PLOT_CONFIG,
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
        st.info("当前筛选下无可展示数据。")
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
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)

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
        st.info("当前筛选下无可用价格带数据。")
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
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)
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
        st.info("当前筛选下无可展示数据。")
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
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)


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
        st.info("当前筛选下无可展示尺寸段数据。")
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
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)


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
        st.info("当前筛选下无可展示数据。")
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
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)
    st.caption(
        "说明：该图基于可调参数进行估算，非财务口径TCO；用于相对比较。"
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
        ["length (mm)", "车长(mm)", "车长", "length"],
    )
    msrp_col, msrp_values = prepare_numeric_axis(
        filtered_df,
        [
            "MSRP规整",
            "MSRP including delivery charge",
            "MSRP",
            "MSRP区间",
        ],
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
        st.info("当前筛选下无可展示气泡图数据。")
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
    st.plotly_chart(fig_bubble, width="stretch", config=PLOT_CONFIG)


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
        st.info("当前筛选下无可用于热力图的月度数据。")
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
    st.plotly_chart(fig_heatmap, width="stretch", config=PLOT_CONFIG)


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
                    "powertrain_price_mix",
                    "estimated_tco_msrp",
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
        }

        group_options = list(chart_groups.keys())
        group_state_key = "advanced_charts_group"
        chart_state_key = "advanced_charts_chart"

        if (
            group_state_key not in st.session_state
            or st.session_state[group_state_key] not in group_options
        ):
            st.session_state[group_state_key] = group_options[0]

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
                if st.button(
                    group_label,
                    key=f"{group_state_key}_btn_{group_key}",
                    help=group_tooltip,
                    type=(
                        "primary"
                        if st.session_state[group_state_key] == group_key
                        else "secondary"
                    ),
                    width="stretch",
                ):
                    st.session_state[group_state_key] = group_key

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
                if st.button(
                    chart_label,
                    key=f"{chart_state_key}_btn_{chart_id}",
                    help=chart_help,
                    type=(
                        "primary"
                        if st.session_state[chart_state_key] == chart_id
                        else "secondary"
                    ),
                    width="stretch",
                ):
                    st.session_state[chart_state_key] = chart_id

        selected_chart = str(st.session_state[chart_state_key])

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

            st.download_button(
                "下载当前预览 CSV",
                data=preview_df.to_csv(index=False).encode("utf-8-sig"),
                file_name="jato_preview.csv",
                mime="text/csv",
                width="content",
            )


def render_dashboard(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selections: FilterSelections,
) -> None:
    render_header_card(filtered_df)
    time_axis = build_time_axis(filtered_df)
    if time_axis:
        global_time_selection = render_global_time_controls(time_axis)
    else:
        global_time_selection = None
        st.warning("未识别到时间列，图表时间轴功能不可用。")

    render_kpi_cards(filtered_df, columns)
    controls = render_line_mode_controls(columns)
    series_order = get_series_order(filtered_df, controls)
    render_top_n_others_detail(filtered_df, controls, series_order)

    year_tab, month_tab = st.tabs(["📅 年度趋势", "🌙 月度细化"])
    with year_tab:
        render_year_tab(
            filtered_df,
            controls,
            series_order,
            time_axis,
            global_time_selection,
        )

    with month_tab:
        render_month_tab(
            filtered_df,
            controls,
            series_order,
            time_axis,
            global_time_selection,
        )

    render_advanced_charts(
        filtered_df,
        columns,
        time_axis,
        global_time_selection,
    )

    render_detail_preview(filtered_df, columns)
