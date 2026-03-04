from pathlib import Path
import re
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="JATO 数据全量可视化分析", layout="wide")


@st.cache_data(show_spinner=False)
def load_full_data():
    base_dir = Path(__file__).resolve().parents[1]  # JATO_Analysis_System
    parquet_path = base_dir / "04_Processed_data" / "jato_full_archive.parquet"

    if not parquet_path.exists():
        st.error(f"未找到数据文件: {parquet_path}")
        st.stop()

    return pd.read_parquet(parquet_path)


def find_col(df: pd.DataFrame, candidates):
    col_map = {str(c).lower().strip(): c for c in df.columns}
    for c in candidates:
        key = str(c).lower().strip()
        if key in col_map:
            return col_map[key]
    return None


def unique_options(df: pd.DataFrame, col: str):
    return sorted(df[col].dropna().astype(str).unique().tolist())


def search_select_filter(label: str, options: list[str], key_prefix: str, max_options: int = 2000):
    """视觉一体卡片：搜索 + 多选 + 全选搜索结果 + 清空"""
    q_key = f"{key_prefix}_q"
    ms_key = f"{key_prefix}_ms"

    # 统一为字符串，避免混合类型问题
    options = [str(o) for o in options]
    all_set = set(options)

    if ms_key not in st.session_state:
        st.session_state[ms_key] = []

    # 只按全量 options 校验已选，避免切关键词后丢失选择
    st.session_state[ms_key] = [x for x in st.session_state[ms_key] if x in all_set]

    with st.sidebar.container(border=True):
        st.markdown(f"**{label}**")

        q = st.text_input(
            f"{label} 搜索",
            key=q_key,
            placeholder=f"输入关键词筛选 {label}",
            label_visibility="collapsed",
        )
        q_lower = q.lower().strip()
        matched = [o for o in options if q_lower in o.lower()] if q_lower else options

        if len(matched) > max_options:
            st.caption(f"匹配项过多，仅显示前 {max_options} 条；请继续缩小关键词。")
            matched = matched[:max_options]

        c1, c2 = st.columns(2)
        if c1.button("全选搜索结果", key=f"{key_prefix}_sel_all", use_container_width=True):
            # 与已有选择做并集，并按原 options 顺序输出
            order = {v: i for i, v in enumerate(options)}
            union_vals = set(st.session_state[ms_key]).union(matched)
            st.session_state[ms_key] = sorted(union_vals, key=lambda v: order.get(v, 10**9))

        if c2.button("清空", key=f"{key_prefix}_clear", use_container_width=True):
            st.session_state[ms_key] = []

        # 显示“已选 + 当前匹配”，保证跨关键词多次选择不丢失
        shown_options = []
        seen = set()
        for x in st.session_state[ms_key] + matched:
            if x not in seen:
                shown_options.append(x)
                seen.add(x)

        selected = st.multiselect(
            label,
            options=shown_options,
            key=ms_key,
            label_visibility="collapsed",
            placeholder=f"选择 {label}",
        )
        st.caption(f"匹配 {len(matched):,} 项｜已选 {len(selected):,} 项")

    return selected


df = load_full_data()

# 列定位（兼容中英文/大小写）
country_col = find_col(df, ["国家", "Country", "country"])
segment_col = find_col(df, ["细分市场（按车长）", "细分市场", "segment"])
powertrain_col = find_col(df, ["动总规整", "powertrain"])
make_col = find_col(df, ["Make", "make", "品牌"])
model_col = find_col(df, ["Model", "model"])
version_col = find_col(df, ["Version name", "Version Name", "version name", "versionname"])

# --- 侧边栏：全维度筛选 ---
st.sidebar.header("🎛️ 全维度筛选")
st.sidebar.caption("每个筛选器均支持：搜索 + 多选 + 全选搜索结果")

if country_col:
    countries = search_select_filter("国家", unique_options(df, country_col), "country")
else:
    countries = []
    st.sidebar.warning("未找到 国家 字段")

if segment_col:
    segments = search_select_filter("细分市场", unique_options(df, segment_col), "segment")
else:
    segments = []
    st.sidebar.warning("未找到 细分市场 字段")

if powertrain_col:
    powertrains = search_select_filter("动总规整", unique_options(df, powertrain_col), "powertrain")
else:
    powertrains = []
    st.sidebar.warning("未找到 动总规整 字段")

# 先应用通用筛选，再做品牌-Model-Version 级联
base_df = df.copy()
if country_col and countries:
    base_df = base_df[base_df[country_col].astype(str).isin(countries)]
if segment_col and segments:
    base_df = base_df[base_df[segment_col].astype(str).isin(segments)]
if powertrain_col and powertrains:
    base_df = base_df[base_df[powertrain_col].astype(str).isin(powertrains)]

# 品牌（第一层）
if make_col:
    makes = search_select_filter("品牌", unique_options(base_df, make_col), "make")
else:
    makes = []
    st.sidebar.warning("未找到 Make/品牌 字段，已跳过品牌筛选")

# Model（第二层：联动品牌）
if model_col:
    model_base = base_df.copy()
    if make_col and makes:
        model_base = model_base[model_base[make_col].astype(str).isin(makes)]
    selected_models = search_select_filter("Model", unique_options(model_base, model_col), "model")
else:
    selected_models = []
    st.sidebar.warning("未找到 Model 字段，已跳过 Model 筛选")

# Version name（第三层：联动品牌+Model）
if version_col:
    version_base = base_df.copy()
    if make_col and makes:
        version_base = version_base[version_base[make_col].astype(str).isin(makes)]
    if model_col and selected_models:
        version_base = version_base[version_base[model_col].astype(str).isin(selected_models)]
    selected_versions = search_select_filter(
        "Version name",
        unique_options(version_base, version_col),
        "version",
        max_options=1500,
    )
else:
    selected_versions = []
    st.sidebar.warning("未找到 Version name 字段，已跳过该筛选")

# 最终过滤
filtered_df = base_df.copy()
if make_col and makes:
    filtered_df = filtered_df[filtered_df[make_col].astype(str).isin(makes)]
if model_col and selected_models:
    filtered_df = filtered_df[filtered_df[model_col].astype(str).isin(selected_models)]
if version_col and selected_versions:
    filtered_df = filtered_df[filtered_df[version_col].astype(str).isin(selected_versions)]

# --- 核心可视化：年度与月度联动 ---
st.title("🚗 JATO 数据全量可视化分析")
st.caption(f"当前筛选结果：{len(filtered_df):,} 行")

tab1, tab2 = st.tabs(["📅 年度趋势", "🌙 月度细化"])

with tab1:
    st.subheader("年度对比")
    year_cols = sorted([c for c in filtered_df.columns if re.fullmatch(r"\d{4}", str(c))])

    if not year_cols:
        st.warning("未识别到年度列（如 2023/2024/2025）。")
    else:
        # Model 有选择 => 每个 Model 一条线；否则显示总计
        split_by_model = bool(model_col and selected_models)

        if split_by_model:
            y_long = filtered_df[[model_col] + year_cols].copy()
            y_long[model_col] = y_long[model_col].astype(str)
            y_long[year_cols] = y_long[year_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

            y_long = y_long.melt(
                id_vars=[model_col],
                value_vars=year_cols,
                var_name="Year",
                value_name="Sales",
            )
            y_plot = y_long.groupby([model_col, "Year"], as_index=False)["Sales"].sum()

            fig_y = px.line(
                y_plot,
                x="Year",
                y="Sales",
                color=model_col,   # 图例=Model
                markers=True,
                title="年度趋势（按 Model）",
            )
        else:
            y_data = filtered_df[year_cols].apply(pd.to_numeric, errors="coerce").sum().reset_index()
            y_data.columns = ["Year", "Sales"]
            fig_y = px.bar(y_data, x="Year", y="Sales", text_auto=".2s", color="Year")

        st.plotly_chart(fig_y, use_container_width=True)

with tab2:
    st.subheader("月度细化（支持时间轴调整）")

    month_pattern = re.compile(
        r"^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
        re.IGNORECASE,
    )
    month_cols = [c for c in filtered_df.columns if month_pattern.match(str(c).strip())]

    if not month_cols:
        st.warning("未识别到月度列（如 '2024 Jan'）。")
    else:
        split_by_model = bool(model_col and selected_models)

        if split_by_model:
            # 每个 Model 一条线
            m_long = filtered_df[[model_col] + month_cols].copy()
            m_long[model_col] = m_long[model_col].astype(str)
            m_long[month_cols] = m_long[month_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

            m_long = m_long.melt(
                id_vars=[model_col],
                value_vars=month_cols,
                var_name="Month",
                value_name="Sales",
            )
            series_col = model_col
        else:
            # 总计单线
            m_sum = filtered_df[month_cols].apply(pd.to_numeric, errors="coerce").sum().reset_index()
            m_sum.columns = ["Month", "Sales"]
            m_sum["_series"] = "总计"
            m_long = m_sum
            series_col = "_series"

        m_long["Date"] = pd.to_datetime(m_long["Month"], format="%Y %b", errors="coerce")
        m_long = m_long.dropna(subset=["Date"]).sort_values("Date")

        if m_long.empty:
            st.info("当前筛选下无可展示月度数据。")
        else:
            min_d = m_long["Date"].min().date()
            max_d = m_long["Date"].max().date()

            c1, c2 = st.columns([2, 1])
            with c1:
                date_range = st.date_input(
                    "选择时间范围",
                    value=(min_d, max_d),
                    min_value=min_d,
                    max_value=max_d,
                )
            with c2:
                axis_level = st.selectbox("时间轴粒度", ["月", "季度", "年"], index=0)

            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_d, end_d = date_range
            else:
                start_d = end_d = date_range

            p = m_long[(m_long["Date"].dt.date >= start_d) & (m_long["Date"].dt.date <= end_d)].copy()

            st.metric("所选时间段销量总和", f"{p['Sales'].sum():,.0f}")

            if axis_level == "月":
                p["Period"] = p["Date"].dt.to_period("M").dt.to_timestamp()
            elif axis_level == "季度":
                p["Period"] = p["Date"].dt.to_period("Q").dt.to_timestamp()
            else:
                p["Period"] = p["Date"].dt.to_period("Y").dt.to_timestamp()

            g = p.groupby([series_col, "Period"], as_index=False)["Sales"].sum()

            if g.empty:
                st.info("该时间范围内无数据。")
            else:
                fig_m = px.line(
                    g,
                    x="Period",
                    y="Sales",
                    color=series_col,  # 图例=Model（或总计）
                    markers=True,
                    title=f"{axis_level}度销量趋势",
                )
                st.plotly_chart(fig_m, use_container_width=True)

# --- 明细数据展示（预览，避免 MessageSizeError）---
with st.expander("🔍 查看明细表（预览）", expanded=False):
    preview_rows = st.slider("预览行数", min_value=100, max_value=5000, value=1000, step=100)

    default_cols = [c for c in [country_col, segment_col, powertrain_col, make_col, model_col, version_col] if c]
    if not default_cols:
        default_cols = filtered_df.columns[:12].tolist()

    show_cols = st.multiselect("显示列", filtered_df.columns.tolist(), default=default_cols)
    preview_df = filtered_df[show_cols].head(preview_rows) if show_cols else filtered_df.head(preview_rows)

    st.dataframe(preview_df, use_container_width=True, height=520)
    if len(filtered_df) > preview_rows:
        st.info(f"仅显示前 {preview_rows:,} 行，完整结果共 {len(filtered_df):,} 行。")