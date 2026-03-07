import pandas as pd
import streamlit as st

from .data import apply_filter_rules, dedupe_preserve_order, unique_options
from .models import ColumnRegistry, FilterSelections


FILTER_KEY_PREFIXES = [
    "country",
    "segment",
    "powertrain",
    "make",
    "model",
    "version",
]

QUERY_PARAM_MAP = {
    "country": "countries",
    "segment": "segments",
    "powertrain": "powertrains",
    "make": "makes",
    "model": "models",
    "version": "versions",
}


def parse_query_param_values(raw_value) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        text = ",".join(str(item) for item in raw_value)
    else:
        text = str(raw_value)
    return [
        item.strip()
        for item in text.split(",")
        if item.strip()
    ]


def hydrate_filter_states_from_query_params_once() -> None:
    hydrated_key = "_filters_query_hydrated"
    if st.session_state.get(hydrated_key):
        return

    for prefix, query_key in QUERY_PARAM_MAP.items():
        values = parse_query_param_values(
            st.query_params.get(query_key)
        )
        if values:
            st.session_state[f"{prefix}_selected"] = values
        if f"{prefix}_query" not in st.session_state:
            st.session_state[f"{prefix}_query"] = ""

    st.session_state[hydrated_key] = True


def sync_query_params_from_selections(
    selections: FilterSelections,
) -> None:
    target_map = {
        "countries": ",".join(selections.countries),
        "segments": ",".join(selections.segments),
        "powertrains": ",".join(selections.powertrains),
        "makes": ",".join(selections.makes),
        "models": ",".join(selections.models),
        "versions": ",".join(selections.versions),
    }
    target_map = {
        key: value
        for key, value in target_map.items()
        if value
    }

    current_map = {
        key: str(st.query_params.get(key))
        for key in target_map.keys()
    }

    has_extra_keys = any(
        key in st.query_params and key not in target_map
        for key in QUERY_PARAM_MAP.values()
    )

    if (current_map == target_map) and not has_extra_keys:
        return

    st.query_params.clear()
    for key, value in target_map.items():
        st.query_params[key] = value


def reset_all_filter_states() -> None:
    for prefix in FILTER_KEY_PREFIXES:
        st.session_state[f"{prefix}_selected"] = []
        st.session_state[f"{prefix}_query"] = ""
    st.query_params.clear()
    st.session_state["_filters_query_hydrated"] = True


def render_search_select_filter(
    label: str,
    options: list[str],
    key_prefix: str,
    max_options: int = 2000,
) -> list[str]:
    """视觉一体卡片：搜索 + 多选 + 全选搜索结果 + 清空。"""
    query_key = f"{key_prefix}_query"
    select_key = f"{key_prefix}_selected"

    normalized_options = [str(option) for option in options]
    option_set = set(normalized_options)

    if select_key not in st.session_state:
        st.session_state[select_key] = []

    st.session_state[select_key] = [
        value for value in st.session_state[select_key] if value in option_set
    ]

    with st.sidebar.container(border=True):
        st.markdown(f"**{label}**")

        query = st.text_input(
            f"{label} 搜索",
            key=query_key,
            placeholder=f"输入关键词筛选 {label}",
            label_visibility="collapsed",
        )

        query_lower = query.lower().strip()
        if query_lower:
            matched_options = [
                option
                for option in normalized_options
                if query_lower in option.lower()
            ]
        else:
            matched_options = normalized_options

        if len(matched_options) > max_options:
            st.caption(f"匹配项过多，仅显示前 {max_options} 条；请继续缩小关键词。")
            matched_options = matched_options[:max_options]

        action_col_1, action_col_2 = st.columns(2)
        if action_col_1.button(
            "全选搜索结果",
            key=f"{key_prefix}_select_all",
            width="stretch",
        ):
            order = {
                value: idx
                for idx, value in enumerate(normalized_options)
            }
            merged_values = set(st.session_state[select_key]).union(
                matched_options
            )
            st.session_state[select_key] = sorted(
                merged_values,
                key=lambda value: order.get(value, len(order)),
            )

        if action_col_2.button(
            "清空",
            key=f"{key_prefix}_clear",
            width="stretch",
        ):
            st.session_state[select_key] = []

        shown_options = dedupe_preserve_order(
            st.session_state[select_key] + matched_options
        )
        selected_values = st.multiselect(
            label,
            options=shown_options,
            key=select_key,
            label_visibility="collapsed",
            placeholder=f"选择 {label}",
        )
        st.caption(
            f"匹配 {len(matched_options):,} 项｜已选 {len(selected_values):,} 项"
        )

    return selected_values


def render_sidebar_filters(
    df: pd.DataFrame,
    columns: ColumnRegistry,
) -> tuple[pd.DataFrame, FilterSelections]:
    hydrate_filter_states_from_query_params_once()

    st.sidebar.header("🎛️ 全维度筛选")
    st.sidebar.caption("每个筛选器均支持：搜索 + 多选 + 全选搜索结果")
    with st.sidebar.container(border=True):
        reset_requested = st.button(
            "重置全部筛选",
            key="filters_reset_all",
            width="stretch",
        )
    if reset_requested:
        reset_all_filter_states()
        st.rerun()

    if columns.country:
        countries = render_search_select_filter(
            "国家",
            unique_options(df, columns.country),
            "country",
        )
    else:
        countries = []
        st.sidebar.warning("未找到 国家 字段")

    if columns.segment:
        segments = render_search_select_filter(
            "细分市场",
            unique_options(df, columns.segment),
            "segment",
        )
    else:
        segments = []
        st.sidebar.warning("未找到 细分市场 字段")

    if columns.powertrain:
        powertrains = render_search_select_filter(
            "动总规整",
            unique_options(df, columns.powertrain),
            "powertrain",
        )
    else:
        powertrains = []
        st.sidebar.warning("未找到 动总规整 字段")

    base_df = apply_filter_rules(
        df,
        [
            (columns.country, countries),
            (columns.segment, segments),
            (columns.powertrain, powertrains),
        ],
    )

    if columns.make:
        makes = render_search_select_filter(
            "品牌",
            unique_options(base_df, columns.make),
            "make",
        )
    else:
        makes = []
        st.sidebar.warning("未找到 Make/品牌 字段，已跳过品牌筛选")

    model_source = apply_filter_rules(base_df, [(columns.make, makes)])
    if columns.model:
        models = render_search_select_filter(
            "Model",
            unique_options(model_source, columns.model),
            "model",
            max_options=800,
        )
    else:
        models = []
        st.sidebar.warning("未找到 Model 字段，已跳过 Model 筛选")

    version_source = apply_filter_rules(
        base_df,
        [
            (columns.make, makes),
            (columns.model, models),
        ],
    )
    if columns.version:
        versions = render_search_select_filter(
            "Version name",
            unique_options(version_source, columns.version),
            "version",
            max_options=500,
        )
    else:
        versions = []
        st.sidebar.warning("未找到 Version name 字段，已跳过该筛选")

    filtered_df = apply_filter_rules(
        base_df,
        [
            (columns.make, makes),
            (columns.model, models),
            (columns.version, versions),
        ],
    )

    selections = FilterSelections(
        countries=countries,
        segments=segments,
        powertrains=powertrains,
        makes=makes,
        models=models,
        versions=versions,
    )
    sync_query_params_from_selections(selections)

    with st.sidebar.container(border=True):
        st.markdown("**📌 筛选摘要**")
        st.caption(
            "｜".join(
                [
                    f"国家 {len(countries):,}",
                    f"细分 {len(segments):,}",
                    f"动总 {len(powertrains):,}",
                    f"品牌 {len(makes):,}",
                    f"Model {len(models):,}",
                    f"Version {len(versions):,}",
                ]
            )
        )
        st.caption(f"当前筛后行数：{len(filtered_df):,}")

    return filtered_df, selections
