from pathlib import Path
import time
from typing import Optional
from uuid import uuid4

import streamlit as st

from .config import (
    APP_TITLE,
    BATTERY_CAPACITY_CANDIDATES,
    BATTERY_RANGE_CANDIDATES,
    MSRP_CANDIDATES,
    LENGTH_CANDIDATES,
    PARQUET_RELATIVE_PATH,
    PARTITIONED_DATASET_RELATIVE_PATH,
)
from .data import (
    dedupe_preserve_order,
    get_dataset_version_token,
    get_project_root,
    get_month_columns_from_names,
    get_year_columns_from_names,
    load_column_names,
    load_dataset_slice,
    resolve_existing_columns,
    resolve_columns_from_names,
)
from .filters import render_sidebar_filters
from .logging_utils import get_logger
from .models import ColumnRegistry, FilterSelections
from .styles import apply_ui_style
from .views import render_dashboard


logger = get_logger("dashboard.runner")
FilterRule = tuple[Optional[str], list[str]]


def configure_page() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        layout="wide",
        page_icon="🚗",
    )


def resolve_data_source_path() -> str:
    project_root = get_project_root()
    full_parquet_path = project_root / PARQUET_RELATIVE_PATH
    partitioned_dir = project_root / PARTITIONED_DATASET_RELATIVE_PATH

    has_partitioned_files = (
        partitioned_dir.exists()
        and any(partitioned_dir.rglob("*.parquet"))
    )
    return str(partitioned_dir if has_partitioned_files else full_parquet_path)


def build_filter_rules(
    columns: ColumnRegistry,
    selections: FilterSelections,
) -> list[FilterRule]:
    return [
        (columns.country, selections.countries),
        (columns.segment, selections.segments),
        (columns.powertrain, selections.powertrains),
        (columns.make, selections.makes),
        (columns.model, selections.models),
        (columns.version, selections.versions),
    ]


def build_filter_payload(
    filter_rules: list[FilterRule],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    payload: list[tuple[str, tuple[str, ...]]] = []
    for column, values in filter_rules:
        if not column or not values:
            continue
        normalized_values = tuple(
            str(value)
            for value in values
            if str(value).strip()
        )
        if not normalized_values:
            continue
        payload.append((str(column), normalized_values))
    return tuple(payload)


def build_analysis_projection(
    column_names: list[str],
    columns: ColumnRegistry,
) -> tuple[str, ...]:
    dimension_columns = [
        columns.country,
        columns.segment,
        columns.powertrain,
        columns.make,
        columns.model,
        columns.version,
    ]
    metric_candidates = [
        *MSRP_CANDIDATES,
        *LENGTH_CANDIDATES,
        *BATTERY_RANGE_CANDIDATES,
        *BATTERY_CAPACITY_CANDIDATES,
    ]
    metric_columns = resolve_existing_columns(
        column_names,
        metric_candidates,
    )
    year_columns = get_year_columns_from_names(column_names)
    month_columns = get_month_columns_from_names(column_names)

    projection_columns = dedupe_preserve_order(
        [
            *(column for column in dimension_columns if column),
            *metric_columns,
            *year_columns,
            *month_columns,
        ]
    )
    return tuple(projection_columns)


def inspect_data_source_health(dataset_path: str) -> list[str]:
    path = Path(dataset_path)
    warnings: list[str] = []

    if not path.exists():
        warnings.append("数据源路径不存在。")
        return warnings

    if path.is_dir():
        parquet_count = sum(1 for _ in path.rglob("*.parquet"))
        if parquet_count == 0:
            warnings.append("分区目录下未发现 Parquet 文件。")

        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            warnings.append("分区目录缺少 manifest.json。")
    else:
        if path.suffix.lower() != ".parquet":
            warnings.append("当前数据源不是 Parquet 文件。")

    return warnings


def main() -> None:
    configure_page()
    apply_ui_style()

    session_id = st.session_state.get("runner_session_id")
    if not session_id:
        session_id = uuid4().hex[:12]
        st.session_state["runner_session_id"] = session_id

    dataset_path = resolve_data_source_path()
    logger.info(
        "session=%s resolved dataset path: %s",
        session_id,
        dataset_path,
    )
    try:
        dataset_version = get_dataset_version_token(dataset_path)
        column_names = load_column_names(
            dataset_path,
            dataset_version=dataset_version,
        )
    except FileNotFoundError as error:
        st.error(str(error))
        st.stop()

    columns = resolve_columns_from_names(column_names)
    health_warnings = inspect_data_source_health(dataset_path)
    for warning in health_warnings:
        logger.warning(
            "session=%s data source health warning: %s",
            session_id,
            warning,
        )
        st.warning(f"数据源检查：{warning}")

    if Path(dataset_path).is_dir() and not columns.country:
        st.warning(
            "分区数据集未识别到“国家”字段，"
            "可能影响分区筛选下推收益。"
        )

    default_large_data_mode = Path(dataset_path).is_dir()
    with st.sidebar.container(border=True):
        st.markdown("**⚙️ 加载模式**")
        large_data_mode = st.toggle(
            "大数据模式（列裁剪 + 过滤下推）",
            value=default_large_data_mode,
            key="runner_large_data_mode",
            help=(
                "开启后仅加载图表所需核心列，适合大数据量；"
                "关闭后读取全部列，适合做全字段明细分析。"
            ),
        )
        detail_on_demand_full_columns = st.toggle(
            "明细预览按需全列",
            value=large_data_mode,
            key="runner_detail_full_columns",
            help="开启后会额外读取全列数据，仅用于明细预览。",
        )

    filter_columns = tuple(
        column
        for column in [
            columns.country,
            columns.segment,
            columns.powertrain,
            columns.make,
            columns.model,
            columns.version,
        ]
        if column
    )

    sidebar_load_start = time.perf_counter()
    sidebar_df = load_dataset_slice(
        parquet_path=dataset_path,
        columns=filter_columns or None,
        dataset_version=dataset_version,
        cache_scope="sidebar",
    )
    sidebar_load_seconds = time.perf_counter() - sidebar_load_start
    _, selections = render_sidebar_filters(sidebar_df, columns)

    filter_rules = build_filter_rules(columns, selections)
    filter_payload = build_filter_payload(filter_rules)
    active_filter_count = sum(1 for _, values in filter_rules if values)
    if large_data_mode:
        analysis_projection = build_analysis_projection(column_names, columns)
    else:
        analysis_projection = tuple(dedupe_preserve_order(column_names))

    analysis_load_start = time.perf_counter()
    analysis_df = load_dataset_slice(
        parquet_path=dataset_path,
        columns=analysis_projection or None,
        filter_payload=filter_payload,
        dataset_version=dataset_version,
        cache_scope="analysis",
    )
    analysis_load_seconds = time.perf_counter() - analysis_load_start

    detail_df = analysis_df
    detail_load_seconds = 0.0
    if detail_on_demand_full_columns and large_data_mode:
        detail_load_start = time.perf_counter()
        detail_df = load_dataset_slice(
            parquet_path=dataset_path,
            columns=None,
            filter_payload=filter_payload,
            dataset_version=dataset_version,
            cache_scope="detail",
        )
        detail_load_seconds = time.perf_counter() - detail_load_start

    analysis_memory_mb = (
        float(analysis_df.memory_usage(deep=True).sum())
        / (1024 * 1024)
    )

    source_type = "分区数据集" if Path(dataset_path).is_dir() else "全量Parquet"
    version_display = (
        dataset_version
        if len(dataset_version) <= 38
        else dataset_version[:38] + "..."
    )
    st.caption(
        f"数据源：{source_type}｜版本：{version_display}｜"
        f"模式：{'大数据' if large_data_mode else '全列'}｜"
        f"读取列数：{len(analysis_projection):,}｜"
        f"侧边栏加载：{sidebar_load_seconds:.2f}s｜"
        f"分析加载：{analysis_load_seconds:.2f}s｜"
        f"明细加载：{detail_load_seconds:.2f}s｜"
        f"侧边栏行数：{len(sidebar_df):,}｜"
        f"分析行数：{len(analysis_df):,}｜"
        f"明细行数：{len(detail_df):,}｜"
        f"分析内存估算：{analysis_memory_mb:.1f}MB"
    )
    logger.info(
        "session=%s mode=%s filters=%s side=%.2fs "
        "analysis=%.2fs detail=%.2fs rows=%s",
        session_id,
        "large" if large_data_mode else "full",
        active_filter_count,
        sidebar_load_seconds,
        analysis_load_seconds,
        detail_load_seconds,
        len(analysis_df),
    )

    filtered_df = analysis_df
    filtered_detail_df = detail_df
    render_dashboard(
        filtered_df,
        columns,
        selections,
        detail_df=filtered_detail_df,
    )
