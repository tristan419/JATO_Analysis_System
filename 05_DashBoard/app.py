from pathlib import Path
import re
import pandas as pd
import streamlit as st
import plotly.express as px

@st.cache_data
def load_full_data():
    base_dir = Path(__file__).resolve().parents[1]  # JATO_Analysis_System
    parquet_path = base_dir / "04_Processed_data" / "jato_full_archive.parquet"

    if not parquet_path.exists():
        st.error(f"未找到数据文件: {parquet_path}")
        st.stop()

    return pd.read_parquet(parquet_path)

def find_col(df: pd.DataFrame, candidates):
    col_map = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        key = c.lower().strip()
        if key in col_map:
            return col_map[key]
    return None

df = load_full_data()

# --- 侧边栏：全维度筛选 ---
st.sidebar.header("🎛️ 全维度筛选")
st.sidebar.caption("支持搜索 + 多选（直接在下拉框中输入关键词）")

countries = st.sidebar.multiselect("国家", sorted(df['国家'].dropna().unique().tolist()))
segments = st.sidebar.multiselect("细分市场", sorted(df['细分市场（按车长）'].dropna().unique().tolist()))
powertrains = st.sidebar.multiselect("动总规整", sorted(df['动总规整'].dropna().unique().tolist()))
makes = st.sidebar.multiselect("品牌", sorted(df['Make'].dropna().unique().tolist()))

# Model 列
model_col = find_col(df, ["model", "Model"])
if model_col:
    model_options = sorted(df[model_col].dropna().astype(str).unique().tolist())
    selected_models = st.sidebar.multiselect("Model", model_options, default=[])
else:
    selected_models = []
    st.sidebar.warning("未找到 'Model' 字段，已跳过 Model 筛选")

# Version name 列（新增）
version_col = find_col(df, ["Version name", "Version Name", "version name", "versionname"])
if version_col:
    version_options = sorted(df[version_col].dropna().astype(str).unique().tolist())
    selected_versions = st.sidebar.multiselect("Version name", version_options, default=[])
else:
    selected_versions = []
    st.sidebar.warning("未找到 'Version name' 字段，已跳过该筛选")

# 执行动态过滤
filtered_df = df.copy()
if countries:
    filtered_df = filtered_df[filtered_df['国家'].isin(countries)]
if segments:
    filtered_df = filtered_df[filtered_df['细分市场（按车长）'].isin(segments)]
if powertrains:
    filtered_df = filtered_df[filtered_df['动总规整'].isin(powertrains)]
if makes:
    filtered_df = filtered_df[filtered_df['Make'].isin(makes)]
if model_col and selected_models:
    filtered_df = filtered_df[filtered_df[model_col].astype(str).isin(selected_models)]
if version_col and selected_versions:
    filtered_df = filtered_df[filtered_df[version_col].astype(str).isin(selected_versions)]

# --- 核心可视化：年度与月度联动 ---
st.title("🚗 JATO 数据全量可视化分析")

tab1, tab2 = st.tabs(["📅 年度趋势", "🌙 月度细化"])

with tab1:
    st.subheader("年度对比")
    year_cols = [c for c in filtered_df.columns if re.fullmatch(r"\d{4}", str(c))]
    year_cols = sorted(year_cols)

    if not year_cols:
        st.warning("未识别到年度列（如 2023/2024/2025）。")
    else:
        y_data = filtered_df[year_cols].apply(pd.to_numeric, errors="coerce").sum().reset_index()
        y_data.columns = ['Year', 'Sales']
        fig_y = px.bar(y_data, x='Year', y='Sales', text_auto='.2s', color='Year')
        st.plotly_chart(fig_y, use_container_width=True)

with tab2:
    st.subheader("月度细化（支持时间轴调整）")

    month_pattern = re.compile(r"^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$", re.IGNORECASE)
    month_cols = [c for c in filtered_df.columns if month_pattern.match(str(c).strip())]

    if not month_cols:
        st.warning("未识别到月度列（如 '2024 Jan'）。")
    else:
        m_data = filtered_df[month_cols].apply(pd.to_numeric, errors="coerce").sum().reset_index()
        m_data.columns = ["Month", "Sales"]

        # 解析 'YYYY Mon' -> datetime
        m_data["Date"] = pd.to_datetime(m_data["Month"], format="%Y %b", errors="coerce")
        m_data = m_data.dropna(subset=["Date"]).sort_values("Date")

        if m_data.empty:
            st.info("当前筛选下无可展示的月度数据。")
        else:
            min_d = m_data["Date"].min().date()
            max_d = m_data["Date"].max().date()

            c1, c2 = st.columns([2, 1])
            with c1:
                date_range = st.date_input(
                    "选择时间范围",
                    value=(min_d, max_d),
                    min_value=min_d,
                    max_value=max_d
                )
            with c2:
                axis_level = st.selectbox("时间轴粒度", ["月", "季度", "年"], index=0)

            # 兼容用户只选了一个日期的情况
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_d, end_d = date_range
            else:
                start_d = end_d = date_range

            p = m_data[(m_data["Date"].dt.date >= start_d) & (m_data["Date"].dt.date <= end_d)].copy()

            # 新增：所选时间段销量总和
            total_sales = p["Sales"].sum()
            st.metric("所选时间段销量总和", f"{total_sales:,.0f}")

            freq_map = {"月": "MS", "季度": "QS", "年": "YS"}
            freq = freq_map[axis_level]

            # 用 resample 保证一定有 Date 列
            g = (
                p.set_index("Date")
                 .resample(freq)["Sales"]
                 .sum()
                 .reset_index()
            )

            if g.empty:
                st.info("该时间范围内无数据。")
            else:
                fig_m = px.line(g, x="Date", y="Sales", markers=True, title=f"{axis_level}度销量趋势")
                st.plotly_chart(fig_m, use_container_width=True)

# --- 明细数据展示 ---
with st.expander("🔍 查看全量明细表 (91列全部保留)"):
    st.dataframe(filtered_df, use_container_width=True)