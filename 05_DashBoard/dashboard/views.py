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
        st.dataframe(others_df, use_container_width=True, height=280)


def render_line_mode_controls(
    columns: ColumnRegistry,
) -> ChartControls:
    with st.container(border=True):
        st.subheader("折线显示模式")
        control_col_1, control_col_2 = st.columns([1, 2])

        with control_col_1:
            chart_mode = st.radio(
                "显示方式",
                ["总和", "分组"],
                horizontal=True,
                key="chart_mode_switch",
                label_visibility="collapsed",
            )

        group_label: str | None = None
        group_column: str | None = None
        top_n_enabled = False
        top_n = 10
        include_others = False
        group_dimensions = get_group_dimensions(columns)

        with control_col_2:
            if chart_mode == "分组":
                if not group_dimensions:
                    st.info("缺少可分组字段，已自动切换为总和模式。")
                    chart_mode = "总和"
                else:
                    group_columns = st.columns([2, 1, 1, 1])
                    (
                        group_col_1,
                        group_col_2,
                        group_col_3,
                        group_col_4,
                    ) = group_columns

                    labels = list(group_dimensions.keys())
                    with group_col_1:
                        group_label = st.selectbox(
                            "分组维度",
                            labels,
                            key="chart_group_dimension",
                        )

                    with group_col_2:
                        top_n_enabled = st.checkbox(
                            "Top N",
                            value=True,
                            key="chart_top_n_enabled",
                            help="仅显示销量前N的分组，其余合并为“其他”",
                        )

                    with group_col_3:
                        top_n = int(
                            st.number_input(
                                "N",
                                min_value=3,
                                max_value=30,
                                value=10,
                                step=1,
                                key="chart_top_n_value",
                                disabled=not top_n_enabled,
                            )
                        )

                    with group_col_4:
                        include_others = st.checkbox(
                            "显示其他",
                            value=False,
                            key="chart_include_others",
                            disabled=not top_n_enabled,
                            help="关闭后图中隐藏“其他”，但可在明细展开查看。",
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
) -> None:
    with st.container(border=True):
        st.subheader("年度对比")
        year_columns = get_year_columns(filtered_df)

        if not year_columns:
            st.warning("未识别到年度列（如 2023/2024/2025）。")
            return

        if controls.chart_mode == "分组" and controls.group_column:
            yearly_long = filtered_df[
                [controls.group_column] + year_columns
            ].copy()
            yearly_long["Series"] = normalize_series(
                yearly_long[controls.group_column]
            )
            yearly_long = yearly_long.melt(
                id_vars=["Series"],
                value_vars=year_columns,
                var_name="Year",
                value_name="Sales",
            )
            yearly_long = apply_top_n_series(
                yearly_long,
                series_order=series_order,
                include_others=controls.include_others,
            )
            year_plot = yearly_long.groupby(
                ["Series", "Year"],
                as_index=False,
            )["Sales"].sum()
            year_plot["Year"] = year_plot["Year"].astype(str)
            color_map = build_color_map(
                year_plot["Series"],
                series_order=series_order,
            )
            title = f"年度趋势（按{controls.group_label}）"

            fig = px.line(
                year_plot,
                x="Year",
                y="Sales",
                color="Series",
                markers=True,
                title=title,
                color_discrete_map=color_map,
            )
        else:
            year_plot = (
                filtered_df[year_columns]
                .sum()
                .rename_axis("Year")
                .reset_index(name="Sales")
            )
            year_plot["Series"] = "总和"
            fig = px.line(
                year_plot,
                x="Year",
                y="Sales",
                color="Series",
                markers=True,
                title="年度趋势（总和）",
                color_discrete_map={"总和": COLOR_SEQ[0]},
            )

        st.plotly_chart(
            style_figure(fig),
            use_container_width=True,
            config=PLOT_CONFIG,
        )


def render_month_tab(
    filtered_df: pd.DataFrame,
    controls: ChartControls,
    series_order: list[str],
) -> None:
    with st.container(border=True):
        st.subheader("月度细化（支持时间轴调整）")

        month_columns = get_month_columns(filtered_df)
        if not month_columns:
            st.warning("未识别到月度列（如 '2024 Jan'）。")
            return

        if controls.chart_mode == "分组" and controls.group_column:
            monthly_long = filtered_df[
                [controls.group_column] + month_columns
            ].copy()
            monthly_long["Series"] = normalize_series(
                monthly_long[controls.group_column]
            )
            monthly_long = monthly_long.melt(
                id_vars=["Series"],
                value_vars=month_columns,
                var_name="Month",
                value_name="Sales",
            )
        else:
            month_total = (
                filtered_df[month_columns]
                .sum()
                .rename_axis("Month")
                .reset_index(name="Sales")
            )
            month_total["Series"] = "总和"
            monthly_long = month_total

        monthly_long["Date"] = pd.to_datetime(
            monthly_long["Month"],
            format="%Y %b",
            errors="coerce",
        )
        monthly_long = monthly_long.dropna(subset=["Date"]).sort_values("Date")

        if monthly_long.empty:
            st.info("当前筛选下无可展示月度数据。")
            return

        min_date = monthly_long["Date"].min().date()
        max_date = monthly_long["Date"].max().date()

        control_col_1, control_col_2 = st.columns([2, 1])
        with control_col_1:
            date_range = st.date_input(
                "选择时间范围",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
        with control_col_2:
            axis_level = st.selectbox("时间轴粒度", ["月", "季度", "年"], index=0)

        if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range

        period_df = monthly_long[
            (monthly_long["Date"].dt.date >= start_date)
            & (monthly_long["Date"].dt.date <= end_date)
        ].copy()
        period_df = apply_top_n_series(
            period_df,
            series_order=series_order,
            include_others=controls.include_others,
        )
        st.metric("所选时间段销量总和", f"{period_df['Sales'].sum():,.0f}")

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
        st.plotly_chart(
            style_figure(fig),
            use_container_width=True,
            config=PLOT_CONFIG,
        )


def render_advanced_charts(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
) -> None:
    with st.container(border=True):
        st.subheader("增强分析图")
        analysis_tab_1, analysis_tab_2 = st.tabs(["动总分布气泡图", "季节性热力图"])

        with analysis_tab_1:
            if not columns.model or not columns.powertrain:
                st.warning("缺少 Model 或 动总规整 字段，无法绘制气泡图。")
            else:
                year_columns = get_year_columns(filtered_df)
                month_columns = get_month_columns(filtered_df)

                if year_columns:
                    sales_ref = filtered_df[year_columns].sum(axis=1)
                elif month_columns:
                    sales_ref = filtered_df[month_columns].sum(axis=1)
                else:
                    sales_ref = pd.Series(1.0, index=filtered_df.index)

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
                else:
                    if columns.make:
                        brand_series = normalize_series(
                            filtered_df[columns.make]
                        )
                    else:
                        brand_series = pd.Series(
                            "全部品牌",
                            index=filtered_df.index,
                        )

                    bubble_raw = pd.DataFrame(
                        {
                            "Model": normalize_series(
                                filtered_df[columns.model]
                            ),
                            "Brand": brand_series,
                            "Powertrain": normalize_series(
                                filtered_df[columns.powertrain]
                            ),
                            "Length": length_values,
                            "MSRP": msrp_values,
                            "Sales": sales_ref,
                        }
                    )
                    bubble_raw = bubble_raw.dropna(
                        subset=["Length", "MSRP", "Sales"]
                    )
                    bubble_raw = bubble_raw[
                        bubble_raw["Powertrain"].isin(POWERTRAIN_DISPLAY_ORDER)
                    ]

                    if bubble_raw.empty:
                        st.info("当前筛选下无 BEV/MHEV/PHEV/ICE/HEV 数据。")
                    else:
                        control_col_1, control_col_2 = st.columns([1, 1])
                        with control_col_1:
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

                        with control_col_2:
                            facet_by_brand = st.checkbox(
                                "按品牌分面对比",
                                value=False,
                                key="bubble_facet_brand",
                                disabled=not bool(columns.make),
                            )
                            max_brand_facets = int(
                                st.number_input(
                                    "最多展示品牌数",
                                    min_value=2,
                                    max_value=12,
                                    value=4,
                                    step=1,
                                    key="bubble_facet_brand_top",
                                    disabled=not facet_by_brand,
                                )
                            )

                        model_rank = (
                            bubble_raw.groupby(
                                "Model", as_index=False
                            )["Sales"]
                            .sum()
                            .sort_values("Sales", ascending=False)
                        )
                        top_models = set(
                            model_rank.head(top_n_models)["Model"]
                        )
                        bubble_raw = bubble_raw[
                            bubble_raw["Model"].isin(top_models)
                        ]

                        facet_col = None
                        category_orders = {
                            "Powertrain": POWERTRAIN_DISPLAY_ORDER,
                        }
                        if facet_by_brand and columns.make:
                            brand_rank = (
                                bubble_raw.groupby(
                                    "Brand", as_index=False
                                )["Sales"]
                                .sum()
                                .sort_values("Sales", ascending=False)
                            )
                            selected_brands = (
                                brand_rank.head(max_brand_facets)["Brand"]
                                .astype(str)
                                .tolist()
                            )
                            bubble_raw = bubble_raw[
                                bubble_raw["Brand"].isin(selected_brands)
                            ]
                            category_orders["Brand"] = selected_brands
                            facet_col = "Brand"

                        group_columns = ["Model", "Powertrain"]
                        if facet_col:
                            group_columns.append("Brand")

                        bubble_df = bubble_raw.groupby(
                            group_columns,
                            as_index=False,
                        ).agg(
                            Length=("Length", "median"),
                            MSRP=("MSRP", "median"),
                            Sales=("Sales", "sum"),
                        )

                        if bubble_df.empty:
                            st.info("当前筛选下无可展示气泡图数据。")
                        else:
                            chart_title = "筛选后 Model 动总分布气泡图"
                            if facet_col:
                                chart_title = (
                                    "筛选后 Model 动总分布气泡图（按品牌分面）"
                                )

                            scatter_kwargs = {}
                            if facet_col:
                                scatter_kwargs["facet_col"] = "Brand"
                                scatter_kwargs["facet_col_wrap"] = 2

                            fig_bubble = px.scatter(
                                bubble_df,
                                x="Length",
                                y="MSRP",
                                size="Sales",
                                color="Powertrain",
                                hover_name="Model",
                                hover_data={
                                    "Sales": ":,.0f",
                                    "Length": ":,.0f",
                                    "MSRP": ":,.0f",
                                },
                                category_orders=category_orders,
                                color_discrete_map=POWERTRAIN_COLOR_MAP,
                                title=chart_title,
                                **scatter_kwargs,
                            )
                            if facet_col:
                                fig_bubble.for_each_annotation(
                                    lambda annotation: annotation.update(
                                        text=annotation.text.replace(
                                            "Brand=", ""
                                        )
                                    )
                                )
                            fig_bubble = style_figure(fig_bubble)
                            fig_bubble.update_xaxes(title=f"车长（{length_col}）")
                            fig_bubble.update_yaxes(title=f"MSRP（{msrp_col}）")

                            if facet_col:
                                st.caption(
                                    f"按品牌分面显示前 {max_brand_facets} 个品牌。"
                                )
                            st.caption(
                                "仅显示 BEV/MHEV/PHEV/ICE/HEV；其余动总类型不显示。"
                            )
                            st.plotly_chart(
                                fig_bubble,
                                use_container_width=True,
                                config=PLOT_CONFIG,
                            )

        with analysis_tab_2:
            month_columns = get_month_columns(filtered_df)
            if not month_columns:
                st.warning("缺少月度列，无法绘制季节性热力图。")
            else:
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
                else:
                    month_total["Year"] = month_total[
                        "Date"
                    ].dt.year.astype(str)
                    month_total["MonthLabel"] = month_total[
                        "Date"
                    ].dt.strftime("%b")

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
                    heatmap_df = heatmap_df.reindex(
                        columns=month_order,
                        fill_value=0,
                    )
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
                    st.plotly_chart(
                        fig_heatmap,
                        use_container_width=True,
                        config=PLOT_CONFIG,
                    )


def render_detail_preview(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
) -> None:
    with st.container(border=True):
        with st.expander("🔍 查看明细表（预览）", expanded=False):
            preview_rows = st.slider(
                "预览行数",
                min_value=100,
                max_value=5000,
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

            preview_df = (
                filtered_df[show_cols].head(preview_rows)
                if show_cols
                else filtered_df.head(preview_rows)
            )

            st.dataframe(preview_df, use_container_width=True, height=520)
            if len(filtered_df) > preview_rows:
                st.info(
                    f"仅显示前 {preview_rows:,} 行，完整结果共 {len(filtered_df):,} 行。"
                )

            st.download_button(
                "下载当前预览 CSV",
                data=preview_df.to_csv(index=False).encode("utf-8-sig"),
                file_name="jato_preview.csv",
                mime="text/csv",
                use_container_width=False,
            )


def render_dashboard(
    filtered_df: pd.DataFrame,
    columns: ColumnRegistry,
    selections: FilterSelections,
) -> None:
    render_header_card(filtered_df)
    render_kpi_cards(filtered_df, columns)
    controls = render_line_mode_controls(columns)
    series_order = get_series_order(filtered_df, controls)
    render_top_n_others_detail(filtered_df, controls, series_order)

    year_tab, month_tab = st.tabs(["📅 年度趋势", "🌙 月度细化"])
    with year_tab:
        render_year_tab(filtered_df, controls, series_order)

    with month_tab:
        render_month_tab(filtered_df, controls, series_order)

    render_advanced_charts(filtered_df, columns)

    render_detail_preview(filtered_df, columns)
