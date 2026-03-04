from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import streamlit as st

from .config import MONTH_COL_PATTERN, YEAR_COL_PATTERN
from .models import ColumnRegistry


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_year_columns(df: pd.DataFrame) -> list[str]:
    year_columns = [
        column
        for column in df.columns
        if YEAR_COL_PATTERN.fullmatch(str(column))
    ]
    return sorted(year_columns)


def get_month_columns(df: pd.DataFrame) -> list[str]:
    return [
        column
        for column in df.columns
        if MONTH_COL_PATTERN.match(str(column).strip())
    ]


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


@st.cache_data(show_spinner=False)
def load_full_data(parquet_path: str) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到数据文件: {path}")

    dataframe = pd.read_parquet(path)
    return optimize_dataframe_types(dataframe)


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
