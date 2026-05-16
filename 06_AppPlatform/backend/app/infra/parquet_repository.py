from collections import OrderedDict
from pathlib import Path
import json
import time
import threading

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

from app.core.config import (
    CRUD_DATA_PATH,
    FILTER_OPTIONS_CACHE_MAX_ENTRIES,
    FILTER_OPTIONS_CACHE_TTL_SECONDS,
    MAX_GROUP_METRICS,
    MAX_RAW_ROWS,
    PARTITIONED_PATH,
    PARQUET_PATH,
    PRECOMPUTED_DIR,
)

# ── Dataset singleton (reuse across requests) ──
_dataset_cache: ds.Dataset | None = None
_dataset_cache_token: str | None = None
_dataset_lock = threading.Lock()


def _resolve_dataset_path() -> Path:
    if (
        PARTITIONED_PATH.is_dir()
        and (PARTITIONED_PATH / "manifest.json").exists()
    ):
        return PARTITIONED_PATH
    if (
        PARTITIONED_PATH.is_dir()
        and next(PARTITIONED_PATH.rglob("*.parquet"), None)
    ):
        return PARTITIONED_PATH
    return PARQUET_PATH


def _dataset_version_sources(path: Path) -> list[Path]:
    candidates = [path]
    if path.is_dir():
        candidates.extend([
            path / "manifest.json",
            path.parent / "manifest.json",
        ])
    else:
        candidates.append(path.parent / "manifest.json")

    sources: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.exists():
            key = str(candidate.resolve())
        else:
            key = str(candidate)
        if key in seen or not candidate.exists():
            continue
        seen.add(key)
        sources.append(candidate)
    return sources or [path]


def current_dataset_token() -> str:
    path = _resolve_dataset_path()
    token_parts: list[str] = []
    for source in _dataset_version_sources(path):
        try:
            stat = source.stat()
        except OSError:
            continue
        token_parts.append(
            f"{source.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        )
    return "|".join(token_parts) or str(path.resolve())


def _open_dataset() -> ds.Dataset:
    global _dataset_cache, _dataset_cache_token
    token = current_dataset_token()
    if _dataset_cache is not None and _dataset_cache_token == token:
        return _dataset_cache
    with _dataset_lock:
        token = current_dataset_token()
        if _dataset_cache is not None and _dataset_cache_token == token:
            return _dataset_cache
        path = _resolve_dataset_path()
        if path.is_file():
            _dataset_cache = ds.dataset(path, format="parquet")
        else:
            _dataset_cache = ds.dataset(
                path,
                format="parquet",
                partitioning="hive",
                exclude_invalid_files=True,
            )
        _dataset_cache_token = token
        return _dataset_cache


# ── Lightweight TTL cache for immutable results ──
_columns_cache: tuple[str, float, list[str]] | None = None
_options_cache: OrderedDict[
    tuple[str, str, tuple[tuple[str, tuple[str, ...]], ...]],
    tuple[float, list[str]],
] = OrderedDict()
_options_cache_lock = threading.Lock()
_METADATA_CACHE_TTL = 300  # 5 minutes


def _normalize_option_values(values: list[object]) -> list[str]:
    return sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and str(value).strip()
        }
    )


def _normalize_filter_cache_key(
    filters: dict[str, list[str]],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    normalized_filters: list[tuple[str, tuple[str, ...]]] = []
    for column, values in filters.items():
        normalized_column = str(column).strip()
        normalized_values = tuple(
            sorted(
                {
                    str(value).strip()
                    for value in (values or [])
                    if value is not None and str(value).strip()
                }
            )
        )
        if not normalized_column or not normalized_values:
            continue
        normalized_filters.append((normalized_column, normalized_values))
    return tuple(sorted(normalized_filters))


def _get_cached_options(
    key: tuple[str, str, tuple[tuple[str, tuple[str, ...]], ...]],
) -> list[str] | None:
    with _options_cache_lock:
        cached = _options_cache.get(key)
        if cached is None:
            return None
        cached_at, options = cached
        if (time.monotonic() - cached_at) >= FILTER_OPTIONS_CACHE_TTL_SECONDS:
            _options_cache.pop(key, None)
            return None
        _options_cache.move_to_end(key)
        return list(options)


def _set_cached_options(
    key: tuple[str, str, tuple[tuple[str, tuple[str, ...]], ...]],
    options: list[str],
) -> None:
    max_entries = max(1, int(FILTER_OPTIONS_CACHE_MAX_ENTRIES))
    with _options_cache_lock:
        _options_cache[key] = (time.monotonic(), list(options))
        _options_cache.move_to_end(key)
        while len(_options_cache) > max_entries:
            _options_cache.popitem(last=False)


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
    global _columns_cache
    now = time.monotonic()
    token = current_dataset_token()
    if _columns_cache is not None:
        cached_token, cached_at, cached_columns = _columns_cache
        if cached_token == token and (now - cached_at) < _METADATA_CACHE_TTL:
            return cached_columns
    dataset = _open_dataset()
    cols = [str(name).strip() for name in dataset.schema.names]
    _columns_cache = (token, now, cols)
    return cols


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
    cache_key = (
        current_dataset_token(),
        str(column).strip(),
        _normalize_filter_cache_key(filters),
    )
    cached_options = _get_cached_options(cache_key)
    if cached_options is not None:
        return cached_options

    options_map = load_distinct_options_batch([column], filters)
    options = options_map.get(column, [])
    _set_cached_options(cache_key, options)
    return options


def load_distinct_options_batch(
    columns: list[str],
    filters: dict[str, list[str]],
) -> dict[str, list[str]]:
    dataset = _open_dataset()
    selected_columns = [
        str(column).strip()
        for column in dict.fromkeys(columns)
        if str(column).strip() in dataset.schema.names
    ]
    if not selected_columns:
        return {}

    table = dataset.to_table(
        columns=selected_columns,
        filter=_build_filter_expression(filters),
    )
    return {
        column: _normalize_option_values(pc.unique(table[column]).to_pylist())
        for column in selected_columns
    }


def count_rows(filters: dict[str, list[str]]) -> int:
    dataset = _open_dataset()
    return int(dataset.count_rows(filter=_build_filter_expression(filters)))


def count_distinct(column: str, filters: dict[str, list[str]]) -> int:
    if not column:
        return 0
    return int(len(load_distinct_options(column, filters)))


def load_slice(
    columns: list[str] | None,
    filters: dict[str, list[str]],
    limit: int | None = MAX_RAW_ROWS,
    offset: int = 0,
) -> pd.DataFrame:
    dataset = _open_dataset()
    scanner = dataset.scanner(
        columns=columns,
        filter=_build_filter_expression(filters),
    )
    if limit is None:
        table = scanner.to_table()
    else:
        start = max(0, int(offset))
        take_n = max(1, int(limit))
        table = scanner.head(start + take_n)
        if start > 0:
            table = table.slice(start)
    return table.to_pandas().reset_index(drop=True)


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
        if len(parts) == 2:
            return int(parts[0]), _MONTH_ORDER.get(parts[1], 0)
        return 0, 0
    return sorted(cols, key=_key)


def _select_latest_time_rows(frame: pd.DataFrame, limit: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.tail(max(1, int(limit))).reset_index(drop=True)


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
        return _select_latest_time_rows(precomputed, limit)

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
    return _select_latest_time_rows(result, top_n)


# ── Country data freshness ──────────────────────────────────────
_COUNTRY_CANDIDATES = ["国家", "Country", "country"]

_freshness_cache: tuple[str, float, list[dict[str, object]]] | None = None
_freshness_lock = threading.Lock()


def country_data_freshness() -> list[dict[str, object]]:
    global _freshness_cache
    now = time.monotonic()
    token = current_dataset_token()
    with _freshness_lock:
        if _freshness_cache is not None:
            cached_token, cached_at, cached_items = _freshness_cache
            if cached_token == token and (now - cached_at) < FILTER_OPTIONS_CACHE_TTL_SECONDS:
                return cached_items

    dataset = _open_dataset()
    all_cols = [str(name).strip() for name in dataset.schema.names]

    country_col: str | None = None
    for candidate in _COUNTRY_CANDIDATES:
        norm = {c.strip().lower(): c.strip() for c in all_cols}
        hit = norm.get(candidate.strip().lower())
        if hit:
            country_col = hit
            break

    month_cols = _sort_month_columns_chrono(_month_columns(all_cols))
    if not country_col or not month_cols:
        return []

    table = dataset.to_table(columns=[country_col, *month_cols])
    df = table.to_pandas()
    if df.empty:
        return []

    for col in month_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    country_totals = df.groupby(country_col, dropna=False)[month_cols].sum()

    items: list[dict[str, object]] = []
    reversed_months = list(reversed(month_cols))
    for country_val, row in country_totals.iterrows():
        latest_month: str | None = None
        for month_col in reversed_months:
            if float(row.get(month_col, 0.0) or 0.0) > 0:
                latest_month = month_col
                break
        if latest_month is None:
            continue
        latest_idx = month_cols.index(latest_month)
        start_idx = max(0, latest_idx - 11)
        items.append({
            "country": str(country_val).strip(),
            "latestMonth": latest_month,
            "monthsInWindow": latest_idx - start_idx + 1,
        })

    items.sort(key=lambda x: str(x.get("country", "")))

    with _freshness_lock:
        _freshness_cache = (token, time.monotonic(), items)

    return items


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
