from typing import Sequence

import streamlit as st

from .data import (
    build_filter_signature,
    dedupe_preserve_order,
    load_distinct_options,
    load_filtered_row_count,
    normalize_filter_payload,
)
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

FilterRule = tuple[str | None, Sequence[str]]
ALLOW_GLOBAL_BRAND_HIERARCHY_KEY = (
    "filters_allow_global_brand_hierarchy"
)


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


def build_filter_payload(
    rules: Sequence[FilterRule],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return normalize_filter_payload(
        [
            (column, values)
            for column, values in rules
            if column and values
        ]
    )


def resolve_pushdown_options(
    parquet_path: str,
    dataset_version: str,
    target_column: str | None,
    rules: Sequence[FilterRule],
) -> list[str]:
    if not target_column:
        return []

    payload = build_filter_payload(rules)
    signature = build_filter_signature(payload)
    return load_distinct_options(
        parquet_path=parquet_path,
        column=target_column,
        filter_payload=payload,
        dataset_version=dataset_version,
        filter_signature=signature,
    )


def render_sidebar_filters(
    parquet_path: str,
    dataset_version: str,
    columns: ColumnRegistry,
) -> tuple[FilterSelections, int]:
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
        country_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.country,
            rules=[],
        )
        countries = render_search_select_filter(
            "国家",
            country_options,
            "country",
        )
    else:
        countries = []
        st.sidebar.warning("未找到 国家 字段")

    if columns.segment:
        segment_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.segment,
            rules=[(columns.country, countries)],
        )
        segments = render_search_select_filter(
            "细分市场",
            segment_options,
            "segment",
        )
    else:
        segments = []
        st.sidebar.warning("未找到 细分市场 字段")

    if columns.powertrain:
        powertrain_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.powertrain,
            rules=[
                (columns.country, countries),
                (columns.segment, segments),
            ],
        )
        powertrains = render_search_select_filter(
            "动总规整",
            powertrain_options,
            "powertrain",
        )
    else:
        powertrains = []
        st.sidebar.warning("未找到 动总规整 字段")

    allow_global_brand_hierarchy = bool(countries)
    if not countries:
        with st.sidebar.container(border=True):
            st.caption(
                "国家为空时默认跳过品牌/Model/Version候选加载，"
                "以减少切换国家时的中间态等待。"
            )
            allow_global_brand_hierarchy = st.toggle(
                "无国家时仍加载品牌/Model/Version（较慢）",
                value=False,
                key=ALLOW_GLOBAL_BRAND_HIERARCHY_KEY,
                help=(
                    "关闭可提升“先删后加国家”场景速度；"
                    "开启可在无国家筛选时做全量品牌层级筛选。"
                ),
            )

    if columns.make and (countries or allow_global_brand_hierarchy):
        make_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.make,
            rules=[
                (columns.country, countries),
                (columns.segment, segments),
                (columns.powertrain, powertrains),
            ],
        )
        makes = render_search_select_filter(
            "品牌",
            make_options,
            "make",
        )
    else:
        makes = []
        if columns.make:
            st.session_state["make_selected"] = []
            st.session_state["model_selected"] = []
            st.session_state["version_selected"] = []
        else:
            st.sidebar.warning("未找到 Make/品牌 字段，已跳过品牌筛选")

    if columns.model and (countries or allow_global_brand_hierarchy):
        model_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.model,
            rules=[
                (columns.country, countries),
                (columns.segment, segments),
                (columns.powertrain, powertrains),
                (columns.make, makes),
            ],
        )
        models = render_search_select_filter(
            "Model",
            model_options,
            "model",
            max_options=800,
        )
    else:
        models = []
        if not columns.model:
            st.sidebar.warning("未找到 Model 字段，已跳过 Model 筛选")

    if columns.version and (countries or allow_global_brand_hierarchy):
        version_options = resolve_pushdown_options(
            parquet_path=parquet_path,
            dataset_version=dataset_version,
            target_column=columns.version,
            rules=[
                (columns.country, countries),
                (columns.segment, segments),
                (columns.powertrain, powertrains),
                (columns.make, makes),
                (columns.model, models),
            ],
        )
        versions = render_search_select_filter(
            "Version name",
            version_options,
            "version",
            max_options=500,
        )
    else:
        versions = []
        if not columns.version:
            st.sidebar.warning("未找到 Version name 字段，已跳过该筛选")

    selections = FilterSelections(
        countries=countries,
        segments=segments,
        powertrains=powertrains,
        makes=makes,
        models=models,
        versions=versions,
    )
    sync_query_params_from_selections(selections)

    summary_payload = build_filter_payload(
        [
            (columns.country, countries),
            (columns.segment, segments),
            (columns.powertrain, powertrains),
            (columns.make, makes),
            (columns.model, models),
            (columns.version, versions),
        ]
    )
    summary_signature = build_filter_signature(summary_payload)
    filtered_row_count = load_filtered_row_count(
        parquet_path=parquet_path,
        filter_payload=summary_payload,
        dataset_version=dataset_version,
        filter_signature=summary_signature,
    )

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
        st.caption(f"当前筛后行数：{filtered_row_count:,}")

    return selections, filtered_row_count
