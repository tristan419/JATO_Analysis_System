from pathlib import Path
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

df = load_full_data()

# --- 侧边栏：全维度筛选 ---
st.sidebar.header("🎛️ 全维度筛选")

# 你的“拖动词条”逻辑：自由组合筛选
countries = st.sidebar.multiselect("国家", df['国家'].unique())
segments = st.sidebar.multiselect("细分市场", df['细分市场（按车长）'].unique())
powertrains = st.sidebar.multiselect("动总规整", df['动总规整'].unique())
makes = st.sidebar.multiselect("品牌", df['Make'].unique())

# 兼容列名大小写（model / Model）
col_map = {c.lower(): c for c in df.columns}
model_col = col_map.get("model")

# 新增：Model 筛选器
if model_col:
    model_options = sorted(df[model_col].dropna().astype(str).unique().tolist())
    selected_models = st.sidebar.multiselect(
        "Model",
        model_options,
        default=model_options
    )
else:
    selected_models = None
    st.sidebar.warning("未找到 'model' 字段，已跳过 Model 筛选")

# 执行动态过滤
query = df.copy()
if countries: query = query[query['国家'].isin(countries)]
if segments: query = query[query['细分市场（按车长）'].isin(segments)]
if powertrains: query = query[query['动总规整'].isin(powertrains)]
if makes: query = query[query['Make'].isin(makes)]

# 现有过滤逻辑基础上，追加 model 条件（其他条件保持不变）
filtered_df = query.copy()

if model_col and selected_models is not None:
    filtered_df = filtered_df[filtered_df[model_col].astype(str).isin(selected_models)]

# --- 核心可视化：年度与月度联动 ---
st.title("🚗 JATO 数据全量可视化分析")

tab1, tab2 = st.tabs(["📅 年度趋势", "🌙 月度细化"])

with tab1:
    st.subheader("2023-2025 年度对比")
    # 动态获取年度列名（假设列名为 '2023', '2024', '2025'）
    year_cols = ['2023', '2024', '2025']
    y_data = filtered_df[year_cols].sum().reset_index()
    y_data.columns = ['Year', 'Sales']
    
    fig_y = px.bar(y_data, x='Year', y='Sales', text_auto='.2s', color='Year')
    st.plotly_chart(fig_y, use_container_width=True)

with tab2:
    st.subheader("2023-2025 全月度波动图")
    # 自动识别格式为 '202x Jan' 的月度列
    month_cols = [col for col in df.columns if any(m in col for m in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])]
    
    m_data = filtered_df[month_cols].sum().T.reset_index()
    m_data.columns = ['Month', 'Sales']
    
    fig_m = px.line(m_data, x='Month', y='Sales', markers=True)
    st.plotly_chart(fig_m, use_container_width=True)

# --- 明细数据展示 ---
with st.expander("🔍 查看全量明细表 (91列全部保留)"):
    st.dataframe(filtered_df)