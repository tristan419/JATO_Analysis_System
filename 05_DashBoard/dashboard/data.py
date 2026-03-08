from pathlib import Path
import hashlib
from typing import Literal, Optional, Sequence

import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
import streamlit as st

from .config import (
    CACHE_MAX_ENTRIES_ANALYSIS,
    CACHE_MAX_ENTRIES_DETAIL,
    CACHE_MAX_ENTRIES_SCHEMA,
    CACHE_MAX_ENTRIES_SIDEBAR,
    CACHE_TTL_ANALYSIS_SECONDS,
    CACHE_TTL_DETAIL_SECONDS,
    CACHE_TTL_SCHEMA_SECONDS,
    CACHE_TTL_SIDEBAR_SECONDS,
    MONTH_COL_PATTERN,
    YEAR_COL_PATTERN,
)
from .models import ColumnRegistry


NormalizedFilterPayload = tuple[tuple[str, tuple[str, ...]], ...]


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_dataset_version_token(parquet_path: str) -> str:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    if path.is_file():
        stat = path.stat()
        return f"file:{stat.st_mtime_ns}:{stat.st_size}"

    manifest_path = path / "manifest.json"
    if manifest_path.exists():
        stat = manifest_path.stat()
        return f"manifest:{stat.st_mtime_ns}:{stat.st_size}"

    parquet_files = sorted(path.rglob("*.parquet"))
    if not parquet_files:
        stat = path.stat()
        return f"dir:{stat.st_mtime_ns}"

    latest_mtime_ns = 0
    total_size = 0
    for file_path in parquet_files:
        stat = file_path.stat()
        total_size += stat.st_size
        latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)

    return (
        f"dataset:{len(parquet_files)}:"
        f"{latest_mtime_ns}:{total_size}"
    )


def get_year_columns(df: pd.DataFrame) -> list[str]:
    year_columns = [
        column
        for column in df.columns
        if YEAR_COL_PATTERN.fullmatch(str(column))
    ]
    return sorted(year_columns)


def get_year_columns_from_names(column_names: Sequence[str]) -> list[str]:
    year_columns = [
        str(column).strip()
        for column in column_names
        if YEAR_COL_PATTERN.fullmatch(str(column).strip())
    ]
    return sorted(year_columns)


def get_month_columns(df: pd.DataFrame) -> list[str]:
    return [
        column
        for column in df.columns
        if MONTH_COL_PATTERN.match(str(column).strip())
    ]


def get_month_columns_from_names(column_names: Sequence[str]) -> list[str]:
    month_columns = [
        str(column).strip()
        for column in column_names
        if MONTH_COL_PATTERN.match(str(column).strip())
    ]

    def parse_month(name: str):
        return pd.to_datetime(
            str(name),
            format="%Y %b",
            errors="coerce",
        )

    return sorted(
        month_columns,
        key=lambda name: (
            parse_month(name).toordinal()
            if pd.notna(parse_month(name))
            else float("inf"),
            name,
        ),
    )


def resolve_existing_columns(
    column_names: Sequence[str],
    candidates: Sequence[str],
) -> list[str]:
    available_map = {
        str(column).strip().lower(): str(column).strip()
        for column in column_names
    }

    resolved: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if not key or key not in available_map:
            continue
        actual = available_map[key]
        if actual in seen:
            continue
        resolved.append(actual)
        seen.add(actual)

    return resolved


def optimize_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    optimized = df.copy()

    optimized.columns = [str(column).strip() for column in optimized.columns]

    dim_candidates = [
        "国家",
        "细分市场（按车长）",
        "动总规整",
        "Make",
        "Model",
        "Version name",
    ]

    for column in dim_candidates:
        if column not in optimized.columns:
            continue

        series = optimized[column].astype("string")
        unique_ratio = series.nunique(dropna=True) / max(len(series), 1)
        if unique_ratio < 0.6:
            optimized[column] = series.astype("category")
        else:
            optimized[column] = series

    numeric_columns = (
        get_year_columns(optimized)
        + get_month_columns(optimized)
    )
    if numeric_columns:
        optimized[numeric_columns] = optimized[numeric_columns].apply(
            pd.to_numeric,
            errors="coerce",
            downcast="float",
        )

    return optimized


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_ANALYSIS_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_ANALYSIS,
)
def load_full_data(
    parquet_path: str,
    dataset_version: str | None = None,
) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    if path.is_file():
        dataframe = pd.read_parquet(path)
    else:
        dataset = open_parquet_dataset(path)
        dataframe = dataset.to_table().to_pandas()
    return optimize_dataframe_types(dataframe)


def open_parquet_dataset(path: Path) -> ds.Dataset:
    if path.is_file():
        return ds.dataset(path, format="parquet")

    return ds.dataset(
        path,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_SCHEMA_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_SCHEMA,
)
def load_column_names(
    parquet_path: str,
    dataset_version: str | None = None,
) -> list[str]:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    dataset = open_parquet_dataset(path)
    return [str(name).strip() for name in dataset.schema.names]


def resolve_columns_from_names(column_names: Sequence[str]) -> ColumnRegistry:
    dataframe = pd.DataFrame(columns=list(column_names))
    return resolve_columns(dataframe)


def normalize_filter_payload(
    filter_payload: Sequence[tuple[str, Sequence[str]]],
) -> NormalizedFilterPayload:
    normalized_payload: list[tuple[str, tuple[str, ...]]] = []
    for column, values in filter_payload:
        normalized_column = str(column).strip()
        if not normalized_column:
            continue

        normalized_values = sorted(
            {
                str(value).strip()
                for value in values
                if str(value).strip()
            }
        )
        if not normalized_values:
            continue

        normalized_payload.append(
            (normalized_column, tuple(normalized_values))
        )

    return tuple(normalized_payload)


def build_filter_signature(
    filter_payload: Sequence[tuple[str, Sequence[str]]],
) -> str:
    normalized_payload = normalize_filter_payload(filter_payload)
    if not normalized_payload:
        return "all"

    signature_material = "&&".join(
        f"{column}={'|'.join(values)}"
        for column, values in normalized_payload
    )
    digest = hashlib.sha1(
        signature_material.encode("utf-8")
    ).hexdigest()
    return f"sig-{digest[:16]}"


def build_arrow_filter_expression(
    filter_payload: Sequence[tuple[str, Sequence[str]]],
) -> ds.Expression | None:
    normalized_payload = normalize_filter_payload(filter_payload)
    expression = None
    for normalized_column, normalized_values in normalized_payload:
        predicate = ds.field(normalized_column).isin(normalized_values)
        if expression is None:
            expression = predicate
        else:
            expression = expression & predicate

    return expression


def _load_dataset_slice_impl(
    parquet_path: str,
    columns: tuple[str, ...] | None = None,
    filter_payload: NormalizedFilterPayload = (),
) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    selected_columns = list(columns) if columns else None
    dataset = open_parquet_dataset(path)
    filter_expression = build_arrow_filter_expression(filter_payload)
    table = dataset.to_table(
        columns=selected_columns,
        filter=filter_expression,
    )
    dataframe = table.to_pandas()

    return optimize_dataframe_types(dataframe)


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_SIDEBAR_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_SIDEBAR,
)
def _load_dataset_slice_sidebar_cached(
    parquet_path: str,
    columns: tuple[str, ...] | None = None,
    filter_payload: NormalizedFilterPayload = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> pd.DataFrame:
    return _load_dataset_slice_impl(
        parquet_path=parquet_path,
        columns=columns,
        filter_payload=filter_payload,
    )


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_ANALYSIS_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_ANALYSIS,
)
def _load_dataset_slice_analysis_cached(
    parquet_path: str,
    columns: tuple[str, ...] | None = None,
    filter_payload: NormalizedFilterPayload = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> pd.DataFrame:
    return _load_dataset_slice_impl(
        parquet_path=parquet_path,
        columns=columns,
        filter_payload=filter_payload,
    )


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_DETAIL_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_DETAIL,
)
def _load_dataset_slice_detail_cached(
    parquet_path: str,
    columns: tuple[str, ...] | None = None,
    filter_payload: NormalizedFilterPayload = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> pd.DataFrame:
    return _load_dataset_slice_impl(
        parquet_path=parquet_path,
        columns=columns,
        filter_payload=filter_payload,
    )


def load_dataset_slice(
    parquet_path: str,
    columns: tuple[str, ...] | None = None,
    filter_payload: Sequence[tuple[str, Sequence[str]]] = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
    cache_scope: Literal["sidebar", "analysis", "detail"] = "analysis",
) -> pd.DataFrame:
    normalized_filter_payload = normalize_filter_payload(filter_payload)

    if cache_scope == "sidebar":
        return _load_dataset_slice_sidebar_cached(
            parquet_path=parquet_path,
            columns=columns,
            filter_payload=normalized_filter_payload,
            dataset_version=dataset_version,
            filter_signature=filter_signature,
        )

    if cache_scope == "detail":
        return _load_dataset_slice_detail_cached(
            parquet_path=parquet_path,
            columns=columns,
            filter_payload=normalized_filter_payload,
            dataset_version=dataset_version,
            filter_signature=filter_signature,
        )

    return _load_dataset_slice_analysis_cached(
        parquet_path=parquet_path,
        columns=columns,
        filter_payload=normalized_filter_payload,
        dataset_version=dataset_version,
        filter_signature=filter_signature,
    )


def _load_distinct_options_impl(
    parquet_path: str,
    column: str,
    filter_payload: NormalizedFilterPayload = (),
) -> list[str]:
    normalized_column = str(column).strip()
    if not normalized_column:
        return []

    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    dataset = open_parquet_dataset(path)
    if normalized_column not in dataset.schema.names:
        return []

    filter_expression = build_arrow_filter_expression(filter_payload)
    table = dataset.to_table(
        columns=[normalized_column],
        filter=filter_expression,
    )

    unique_values = pc.unique(table[normalized_column]).to_pylist()
    normalized_options = sorted(
        {
            str(value).strip()
            for value in unique_values
            if value is not None and str(value).strip()
        }
    )
    return normalized_options


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_SIDEBAR_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_SIDEBAR,
)
def _load_distinct_options_sidebar_cached(
    parquet_path: str,
    column: str,
    filter_payload: NormalizedFilterPayload = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> list[str]:
    return _load_distinct_options_impl(
        parquet_path=parquet_path,
        column=column,
        filter_payload=filter_payload,
    )


def load_distinct_options(
    parquet_path: str,
    column: str,
    filter_payload: Sequence[tuple[str, Sequence[str]]] = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> list[str]:
    normalized_filter_payload = normalize_filter_payload(filter_payload)
    return _load_distinct_options_sidebar_cached(
        parquet_path=parquet_path,
        column=column,
        filter_payload=normalized_filter_payload,
        dataset_version=dataset_version,
        filter_signature=filter_signature,
    )


@st.cache_data(
    show_spinner=False,
    ttl=CACHE_TTL_SIDEBAR_SECONDS,
    max_entries=CACHE_MAX_ENTRIES_SIDEBAR,
)
def _load_filtered_row_count_sidebar_cached(
    parquet_path: str,
    filter_payload: NormalizedFilterPayload = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> int:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    dataset = open_parquet_dataset(path)
    filter_expression = build_arrow_filter_expression(filter_payload)
    return int(dataset.count_rows(filter=filter_expression))


def load_filtered_row_count(
    parquet_path: str,
    filter_payload: Sequence[tuple[str, Sequence[str]]] = (),
    dataset_version: str | None = None,
    filter_signature: str | None = None,
) -> int:
    normalized_filter_payload = normalize_filter_payload(filter_payload)
    return _load_filtered_row_count_sidebar_cached(
        parquet_path=parquet_path,
        filter_payload=normalized_filter_payload,
        dataset_version=dataset_version,
        filter_signature=filter_signature,
    )


def find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    col_map = {str(column).lower().strip(): column for column in df.columns}
    for candidate in candidates:
        key = str(candidate).lower().strip()
        if key in col_map:
            return col_map[key]
    return None


def resolve_columns(df: pd.DataFrame) -> ColumnRegistry:
    return ColumnRegistry(
        country=find_column(df, ["国家", "Country", "country"]),
        segment=find_column(df, ["细分市场（按车长）", "细分市场", "segment"]),
        powertrain=find_column(df, ["动总规整", "powertrain"]),
        make=find_column(df, ["Make", "make", "品牌"]),
        model=find_column(df, ["Model", "model"]),
        version=find_column(
            df,
            ["Version name", "Version Name", "version name", "versionname"],
        ),
    )


def unique_options(df: pd.DataFrame, column: str) -> list[str]:
    return sorted(df[column].dropna().astype(str).unique().tolist())


def dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def apply_filter_rules(
    df: pd.DataFrame,
    rules: Sequence[tuple[Optional[str], Sequence[str]]],
) -> pd.DataFrame:
    if df.empty:
        return df

    mask = pd.Series(True, index=df.index)
    for column, selected_values in rules:
        if not column or not selected_values:
            continue
        mask &= df[column].astype(str).isin(selected_values)

    return df.loc[mask]
