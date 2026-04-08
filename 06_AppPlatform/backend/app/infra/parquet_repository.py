from pathlib import Path
import json

import pandas as pd
import pyarrow.dataset as ds

from app.core.config import (
    CRUD_DATA_PATH,
    MAX_GROUP_METRICS,
    MAX_RAW_ROWS,
    PARTITIONED_PATH,
    PARQUET_PATH,
    PRECOMPUTED_DIR,
)


def _resolve_dataset_path() -> Path:
    if PARTITIONED_PATH.exists() and any(PARTITIONED_PATH.rglob("*.parquet")):
        return PARTITIONED_PATH
    return PARQUET_PATH


def _open_dataset() -> ds.Dataset:
    path = _resolve_dataset_path()
    if path.is_file():
        return ds.dataset(path, format="parquet")
    return ds.dataset(
        path,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )


def _build_filter_expression(
    filters: dict[str, list[str]],
) -> ds.Expression | None:
    expr = None
    for col, values in filters.items():
        normalized = [str(v).strip() for v in values if str(v).strip()]
        if not col or not normalized:
            continue
        predicate = ds.field(str(col).strip()).isin(normalized)
        expr = predicate if expr is None else (expr & predicate)
    return expr


def list_columns() -> list[str]:
    dataset = _open_dataset()
    return [str(name).strip() for name in dataset.schema.names]


def load_precomputed(summary_name: str) -> pd.DataFrame:
    path = PRECOMPUTED_DIR / f"{summary_name}_summary.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_distinct_options(
    column: str,
    filters: dict[str, list[str]],
) -> list[str]:
    dataset = _open_dataset()
    if column not in dataset.schema.names:
        return []
    table = dataset.to_table(
        columns=[column],
        filter=_build_filter_expression(filters),
    )
    values = table[column].to_pylist()
    return sorted(
        {
            str(v).strip()
            for v in values
            if v is not None and str(v).strip()
        }
    )


def count_rows(filters: dict[str, list[str]]) -> int:
    dataset = _open_dataset()
    return int(dataset.count_rows(filter=_build_filter_expression(filters)))


def count_distinct(column: str, filters: dict[str, list[str]]) -> int:
    dataset = _open_dataset()
    if column not in dataset.schema.names:
        return 0
    table = dataset.to_table(
        columns=[column],
        filter=_build_filter_expression(filters),
    )
    values = table[column].to_pylist()
    normalized = {
        str(v).strip()
        for v in values
        if v is not None and str(v).strip()
    }
    return int(len(normalized))


def load_slice(
    columns: list[str] | None,
    filters: dict[str, list[str]],
    limit: int = MAX_RAW_ROWS,
    offset: int = 0,
) -> pd.DataFrame:
    dataset = _open_dataset()
    table = dataset.to_table(
        columns=columns,
        filter=_build_filter_expression(filters),
    )
    df = table.to_pandas()
    start = max(0, int(offset))
    end = start + max(1, int(limit))
    return df.iloc[start:end].reset_index(drop=True)


def aggregate(
    group_by: str,
    metric_candidates: list[str],
    filters: dict[str, list[str]],
    top_n: int,
) -> pd.DataFrame:
    dataset = _open_dataset()
    metric_cols = [
        col for col in metric_candidates
        if col in dataset.schema.names and col != group_by
    ][:MAX_GROUP_METRICS]

    selected = [group_by, *metric_cols]
    table = dataset.to_table(
        columns=selected,
        filter=_build_filter_expression(filters),
    )
    df = table.to_pandas()
    if df.empty or group_by not in df.columns:
        return df

    numeric_metrics = [
        col for col in metric_cols
        if pd.api.types.is_numeric_dtype(df[col])
    ]

    if numeric_metrics:
        agg_spec = {col: "mean" for col in numeric_metrics}
        grouped = (
            df.groupby(group_by, dropna=False)
            .agg(agg_spec)
            .reset_index()
        )
    else:
        grouped = df[[group_by]].drop_duplicates().reset_index(drop=True)

    counts = (
        df.groupby(group_by, dropna=False)
        .size()
        .reset_index(name="count")
    )
    merged = grouped.merge(counts, on=group_by, how="left")
    return merged.sort_values("count", ascending=False).head(top_n)


def _month_columns(columns: list[str]) -> list[str]:
    return [
        col for col in columns
        if len(col) >= 8 and col[:4].isdigit() and " " in col
    ]


_MONTH_ORDER = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _sort_month_columns_chrono(cols: list[str]) -> list[str]:
    """Sort month columns like '2023 Jan' in chronological order."""
    def _key(col: str) -> tuple[int, int]:
        parts = col.split(" ", 1)
        return (int(parts[0]), _MONTH_ORDER.get(parts[1], 0)) if len(parts) == 2 else (0, 0)
    return sorted(cols, key=_key)


def _year_columns(columns: list[str]) -> list[str]:
    return [
        col for col in columns
        if len(col) == 4 and col.isdigit()
    ]


def time_series(
    filters: dict[str, list[str]],
    grain: str,
    top_n: int,
) -> pd.DataFrame:
    precomputed = load_precomputed("yearMonth")
    if grain == "month" and not filters and not precomputed.empty:
        limit = max(1, int(top_n))
        return precomputed.head(limit)

    dataset = _open_dataset()
    all_cols = [str(name).strip() for name in dataset.schema.names]
    if grain == "year":
        time_cols = sorted(_year_columns(all_cols))
    else:
        time_cols = _sort_month_columns_chrono(_month_columns(all_cols))

    if not time_cols:
        return pd.DataFrame(columns=["time", "value"])

    table = dataset.to_table(
        columns=time_cols,
        filter=_build_filter_expression(filters),
    )
    df = table.to_pandas()
    if df.empty:
        return pd.DataFrame(columns=["time", "value"])

    payload: list[dict[str, float | str]] = []
    for col in time_cols:
        numeric = pd.to_numeric(df[col], errors="coerce")
        payload.append({
            "time": col,
            "value": float(numeric.sum(skipna=True)),
        })

    result = pd.DataFrame(payload)
    return result.head(max(1, int(top_n)))


def _read_crud_items() -> list[dict]:
    if not CRUD_DATA_PATH.exists():
        return []
    try:
        return json.loads(CRUD_DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _write_crud_items(items: list[dict]) -> None:
    CRUD_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRUD_DATA_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def list_crud_items() -> list[dict]:
    return _read_crud_items()


def list_crud_items_query(
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
    query: str,
) -> dict:
    items = _read_crud_items()

    keyword = str(query).strip().lower()
    if keyword:
        items = [
            row
            for row in items
            if (
                keyword in str(row.get("code", "")).lower()
                or keyword in str(row.get("name", "")).lower()
                or keyword in str(row.get("status", "")).lower()
                or keyword in str(row.get("notes", "")).lower()
            )
        ]

    sort_key = str(sort_by).strip().lower()
    reverse = str(sort_order).strip().lower() == "desc"

    def _id_key(row: dict) -> str:
        return str(row.get("id", ""))

    def _field_key(row: dict) -> str:
        return str(row.get(sort_key, "")).lower()

    if sort_key == "created":
        key_func = _id_key
    elif sort_key == "updated":
        key_func = _id_key
    else:
        key_func = _field_key

    items = sorted(items, key=key_func, reverse=reverse)

    total = len(items)
    page_num = max(1, int(page))
    size = max(1, int(page_size))
    start = (page_num - 1) * size
    end = start + size

    return {
        "page": page_num,
        "pageSize": size,
        "total": int(total),
        "items": items[start:end],
    }


def upsert_crud_item(item: dict) -> dict:
    items = _read_crud_items()
    idx = next(
        (
            i for i, row in enumerate(items)
            if row.get("id") == item.get("id")
        ),
        None,
    )
    if idx is None:
        items.append(item)
    else:
        items[idx] = item
    _write_crud_items(items)
    return item


def delete_crud_item(item_id: str) -> bool:
    items = _read_crud_items()
    new_items = [row for row in items if str(row.get("id")) != str(item_id)]
    if len(new_items) == len(items):
        return False
    _write_crud_items(new_items)
    return True
