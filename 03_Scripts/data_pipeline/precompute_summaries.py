"""预聚合模块。

支持两种模式：
1. 全量预聚合（冷启动或兜底）
2. 增量预聚合（当前实现先覆盖国家汇总）
"""

import json
import argparse
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_analysis_data(parquet_path: str) -> pd.DataFrame:
    """从 parquet 加载分析数据。"""
    if Path(parquet_path).is_dir():
        dataset = ds.dataset(parquet_path, format="parquet")
        return dataset.to_table().to_pandas()
    else:
        return pd.read_parquet(parquet_path)


def count_analysis_rows(parquet_path: str) -> int:
    """Read row counts from parquet metadata without materializing the dataset."""
    path = Path(parquet_path)
    if path.is_dir():
        return int(ds.dataset(path, format="parquet").count_rows())
    return int(pq.ParquetFile(path).metadata.num_rows)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _duckdb_summary_query(
    connection: Any,
    *,
    group_column: str,
    numeric_columns: list[str],
    include_min_max: bool,
    include_numeric_count: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    group = _quote_identifier(group_column)
    projections = [group, f"count({group}) AS {_quote_identifier(group_column + '_count')}"]
    order_terms = [f"count({group}) DESC"] if limit else [f"{group} ASC"]
    for column in numeric_columns:
        quoted = _quote_identifier(column)
        projections.append(
            f"avg({quoted}) AS {_quote_identifier(column + '_mean')}"
        )
        if include_min_max:
            projections.extend(
                [
                    f"min({quoted}) AS {_quote_identifier(column + '_min')}",
                    f"max({quoted}) AS {_quote_identifier(column + '_max')}",
                    f"count({quoted}) AS {_quote_identifier(column + '_count')}",
                ]
            )
        elif include_numeric_count:
            projections.append(
                f"count({quoted}) AS {_quote_identifier(column + '_count')}"
            )
        if limit:
            order_terms.append(f"avg({quoted}) DESC NULLS LAST")
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return connection.execute(
        "SELECT "
        + ", ".join(projections)
        + " FROM jato_source"
        + f" WHERE {group} IS NOT NULL"
        + f" GROUP BY {group}"
        + " ORDER BY "
        + ", ".join(order_terms)
        + limit_sql
    ).df()


def compute_all_summaries_bounded(
    parquet_path: str,
    *,
    scratch_parent: Path,
) -> dict[str, pd.DataFrame]:
    """Aggregate the full archive in DuckDB with disk spill and one thread."""
    import duckdb
    import pyarrow as pa

    source = Path(parquet_path)
    schema = (
        ds.dataset(source, format="parquet").schema
        if source.is_dir()
        else pq.read_schema(source)
    )
    columns = [str(column) for column in schema.names]
    numeric_columns = [
        field.name
        for field in schema
        if (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_decimal(field.type)
        )
    ]
    month_columns = sorted(
        [column for column in columns if column.startswith("20") and len(column) > 4]
    )
    source_glob = str(source / "**" / "*.parquet") if source.is_dir() else str(source)
    escaped_source = source_glob.replace("'", "''")
    scratch_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".jato-summary-duckdb-",
        dir=scratch_parent,
    ) as scratch_dir:
        connection = duckdb.connect(database=":memory:")
        try:
            escaped_scratch = str(scratch_dir).replace("'", "''")
            connection.execute("SET threads=1")
            connection.execute("SET memory_limit='512MB'")
            connection.execute("SET preserve_insertion_order=false")
            connection.execute(
                f"SET temp_directory='{escaped_scratch}'"
            )
            connection.execute(
                "CREATE VIEW jato_source AS SELECT * FROM "
                f"read_parquet('{escaped_source}')"
            )

            country_summary = (
                _duckdb_summary_query(
                    connection,
                    group_column="国家",
                    numeric_columns=numeric_columns,
                    include_min_max=True,
                )
                if "国家" in columns
                else pd.DataFrame()
            )
            if month_columns:
                month_projection: list[str] = []
                for index, column in enumerate(month_columns):
                    quoted = _quote_identifier(column)
                    month_projection.extend(
                        [
                            f"sum({quoted}) AS total_{index}",
                            f"avg({quoted}) AS average_{index}",
                        ]
                    )
                month_values = connection.execute(
                    "SELECT "
                    + ", ".join(month_projection)
                    + " FROM jato_source"
                ).fetchone()
                year_month_summary = pd.DataFrame(
                    [
                        {
                            "yearMonth": column,
                            "totalCount": int(month_values[index * 2] or 0),
                            "avgValue": float(
                                month_values[index * 2 + 1] or 0
                            ),
                        }
                        for index, column in enumerate(month_columns)
                    ]
                )
            else:
                year_month_summary = pd.DataFrame()

            powertrain_column = (
                "动力系统"
                if "动力系统" in columns
                else "Powertrain"
                if "Powertrain" in columns
                else None
            )
            powertrain_summary = (
                _duckdb_summary_query(
                    connection,
                    group_column=powertrain_column,
                    numeric_columns=numeric_columns[:5],
                    include_min_max=False,
                )
                if powertrain_column
                else pd.DataFrame()
            )
            segment_column = (
                "车形分类"
                if "车形分类" in columns
                else "Segment"
                if "Segment" in columns
                else None
            )
            segment_summary = (
                _duckdb_summary_query(
                    connection,
                    group_column=segment_column,
                    numeric_columns=numeric_columns[:5],
                    include_min_max=False,
                    include_numeric_count=False,
                )
                if segment_column
                else pd.DataFrame()
            )
            make_column = (
                "品牌"
                if "品牌" in columns
                else "Make"
                if "Make" in columns
                else None
            )
            top_makes_summary = (
                _duckdb_summary_query(
                    connection,
                    group_column=make_column,
                    numeric_columns=numeric_columns[:3],
                    include_min_max=False,
                    include_numeric_count=False,
                    limit=20,
                )
                if make_column
                else pd.DataFrame()
            )
        finally:
            connection.close()
    return {
        "country": country_summary,
        "yearMonth": year_month_summary,
        "powertrain": powertrain_summary,
        "segment": segment_summary,
        "topMakes": top_makes_summary,
    }


def load_existing_summary(
    output_dir: str,
    summary_name: str,
) -> pd.DataFrame:
    path = Path(output_dir) / f"{summary_name}_summary.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def list_current_country_keys(partitioned_dataset_path: str) -> set[str]:
    root = Path(partitioned_dataset_path)
    if not root.exists():
        return set()

    keys: set[str] = set()
    for path in root.rglob("国家=*"):
        if not path.is_dir():
            continue
        suffix = path.name.split("=", 1)[-1]
        key = unquote(suffix).strip()
        if key:
            keys.add(key)
    return keys


def build_country_partition_path(
    partitioned_dataset_path: str,
    country_key: str,
) -> Path:
    encoded = quote(str(country_key), safe="")
    return Path(partitioned_dataset_path) / f"国家={encoded}"


def compute_country_summary_incremental(
    partitioned_dataset_path: str,
    output_dir: str,
    changed_country_keys: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """按变化国家分区增量更新国家汇总。

    当前实现只增量更新 country_summary，其他汇总保留现有或兜底全量重算。
    """
    existing = load_existing_summary(output_dir, "country")
    current_keys = list_current_country_keys(partitioned_dataset_path)

    normalized_changed = {
        str(item).strip() for item in changed_country_keys if str(item).strip()
    }
    if not normalized_changed:
        return existing, {
            "mode": "full",
            "reason": "no-changed-keys",
            "changedCountryCount": 0,
            "recomputedCountryCount": 0,
            "removedCountryCount": 0,
        }

    existing_keys = (
        {
            str(item).strip()
            for item in existing["国家"].dropna().tolist()
            if str(item).strip()
        }
        if not existing.empty and "国家" in existing.columns
        else set()
    )
    missing_existing_keys = current_keys - existing_keys
    keys_to_recompute = sorted(
        (normalized_changed & current_keys) | missing_existing_keys
    )
    removed_keys = sorted(
        (normalized_changed - current_keys) | (existing_keys - current_keys)
    )

    refreshed_rows: list[pd.DataFrame] = []
    for country_key in keys_to_recompute:
        partition_path = build_country_partition_path(
            partitioned_dataset_path,
            country_key,
        )
        if not partition_path.exists():
            continue
        try:
            dataset = ds.dataset(str(partition_path), format="parquet")
            partition_df = dataset.to_table().to_pandas()
        except Exception as error:
            raise ValueError(
                f"国家分区读取失败，不能生成完整 summary: {country_key}"
            ) from error
        # Partition files intentionally omit the partition column. Restore it
        # from the directory key before aggregating the country row.
        if "国家" not in partition_df.columns:
            partition_df["国家"] = country_key
        partition_summary = compute_country_summary(partition_df)
        if partition_summary.empty:
            raise ValueError(
                f"国家分区无法生成 summary: {country_key}"
            )
        refreshed_rows.append(partition_summary)

    refreshed_df = (
        pd.concat(refreshed_rows, ignore_index=True)
        if refreshed_rows
        else pd.DataFrame(columns=existing.columns)
    )

    if not existing.empty and "国家" in existing.columns:
        country_series = existing["国家"].astype(str).str.strip()
        reusable_keys = current_keys - set(keys_to_recompute)
        existing = existing[country_series.isin(reusable_keys)]

    merged = pd.concat([existing, refreshed_df], ignore_index=True)
    if "国家" in merged.columns:
        merged = merged.sort_values("国家").reset_index(drop=True)

    merged_keys = (
        {
            str(item).strip()
            for item in merged["国家"].dropna().tolist()
            if str(item).strip()
        }
        if "国家" in merged.columns
        else set()
    )
    if merged_keys != current_keys or len(merged) != len(current_keys):
        missing_keys = sorted(current_keys - merged_keys)
        unexpected_keys = sorted(merged_keys - current_keys)
        raise ValueError(
            "增量国家 summary 与当前分区不一致："
            f"missing={missing_keys}, unexpected={unexpected_keys}, "
            f"summaryRows={len(merged)}, partitionCountries={len(current_keys)}"
        )

    return merged, {
        "mode": "incremental-country",
        "reason": "changed-country-keys",
        "changedCountryCount": len(normalized_changed),
        "recomputedCountryCount": len(keys_to_recompute),
        "backfilledCountryCount": len(missing_existing_keys),
        "removedCountryCount": len(removed_keys),
        "changedCountryKeys": sorted(normalized_changed),
        "recomputedCountryKeys": keys_to_recompute,
        "backfilledCountryKeys": sorted(missing_existing_keys),
        "removedCountryKeys": removed_keys,
    }


def compute_country_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按国家聚合：计数、平均值等。"""
    if "国家" not in df.columns:
        return pd.DataFrame()

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())

    agg_dict = {"国家": "count"}
    for col in numeric_cols:
        if col not in ["国家"]:
            agg_dict[col] = ["mean", "min", "max", "count"]

    summary = df.groupby("国家").agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_year_month_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按年月聚合：逐月数据。"""
    month_cols = [col for col in df.columns
                  if col.startswith("20") and len(col) > 4]

    if not month_cols:
        return pd.DataFrame()

    monthly_data = []
    for col in sorted(month_cols):
        if col in df.columns:
            monthly_data.append({
                "yearMonth": col,
                "totalCount": int(df[col].sum()),
                "avgValue": float(df[col].mean()),
            })

    return pd.DataFrame(monthly_data)


def compute_powertrain_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按功率系统（燃油、混动、纯电等）聚合。"""
    if ("动力系统" not in df.columns
            and "Powertrain" not in df.columns):
        return pd.DataFrame()

    pt_col = ("动力系统" if "动力系统" in df.columns
              else "Powertrain")

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())
    agg_dict = {pt_col: "count"}
    for col in numeric_cols[:5]:  # Only first 5 metrics
        agg_dict[col] = ["mean", "count"]

    summary = df.groupby(pt_col).agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按车形分类聚合。"""
    if ("车形分类" not in df.columns
            and "Segment" not in df.columns):
        return pd.DataFrame()

    seg_col = ("车形分类" if "车形分类" in df.columns
               else "Segment")

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())
    agg_dict = {seg_col: "count"}
    for col in numeric_cols[:5]:
        agg_dict[col] = ["mean"]

    summary = df.groupby(seg_col).agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_top_makes_summary(
    df: pd.DataFrame,
    top_n: int = 20,
) -> pd.DataFrame:
    """计算最热门的 N 个品牌。"""
    if ("品牌" not in df.columns
            and "Make" not in df.columns):
        return pd.DataFrame()

    make_col = ("品牌" if "品牌" in df.columns
                else "Make")
    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())

    agg_dict = {make_col: "count"}
    for col in numeric_cols[:3]:
        agg_dict[col] = "mean"

    summary = df.groupby(make_col).agg(agg_dict)
    summary = (summary.sort_values(
        by=list(agg_dict.keys()),
        ascending=False).head(top_n))
    summary = summary.reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def save_summary_tables(
    summaries: dict[str, pd.DataFrame],
    output_dir: str,
) -> dict[str, str]:
    """保存预聚合表到 Parquet 和 CSV。"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    saved_files = {}
    for name, df in summaries.items():
        if df.empty:
            continue

        csv_path = output_path / f"{name}_summary.csv"
        parquet_path = output_path / f"{name}_summary.parquet"

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        df.to_parquet(parquet_path, index=False)

        saved_files[name] = {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "rows": len(df),
            "columns": len(df.columns),
        }

    return saved_files


def generate_summaries_manifest(
    saved_files: dict[str, dict],
    original_row_count: int,
    precompute_mode: str = "full",
    incremental_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """生成预聚合清单。"""
    total_summary_rows = sum(
        item.get("rows", 0)
        for item in saved_files.values()
    )

    bandwidth_reduction = (1.0 -
                           (total_summary_rows /
                            max(original_row_count, 1)))

    return {
        "generatedAt": pd.Timestamp.now().isoformat(),
        "originalRowCount": original_row_count,
        "totalSummaryRows": total_summary_rows,
        "bandwidthReduction": f"{bandwidth_reduction * 100:.1f}%",
        "precomputeMode": precompute_mode,
        "incremental": incremental_info or {
            "mode": "full",
        },
        "summaries": saved_files,
    }


def precompute_all_summaries(
    parquet_path: str,
    output_dir: str = "04_Processed_data/summaries",
    partitioned_dataset_path: str | None = None,
    changed_partition_keys: list[str] | None = None,
    existing_country_summary_dir: str | None = None,
) -> dict[str, Any]:
    """
    主入口：加载数据并预聚合所有统计汇总。

    ``existing_country_summary_dir`` 仅作为未变化国家的只读来源；
    其余 summary 始终从 ``output_dir`` 读取或基于当前 parquet 重算。
    返回清单信息。
    """
    changed_keys = changed_partition_keys or []
    incremental_country_enabled = bool(
        partitioned_dataset_path and changed_keys
    )
    country_summary_source_dir = existing_country_summary_dir or output_dir

    print(f"📊 开始预聚合：读取 {parquet_path}...")

    # Row count comes from parquet metadata. Loading the full archive just to count
    # rows previously doubled the candidate worker's peak memory.
    original_rows = count_analysis_rows(parquet_path)
    if not incremental_country_enabled:
        print("📊 使用 DuckDB 单线程流式聚合并允许磁盘溢写...")
        summaries = compute_all_summaries_bounded(
            parquet_path,
            scratch_parent=Path(output_dir).parent,
        )
        print(f"\n💾 保存预聚合表到 {output_dir}...")
        saved_files = save_summary_tables(summaries, output_dir)
        manifest = generate_summaries_manifest(
            saved_files=saved_files,
            original_row_count=original_rows,
            precompute_mode="full",
            incremental_info={"mode": "full"},
        )
        manifest_path = Path(output_dir) / "summaries_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"   ✓ 清单已保存: {manifest_path}")
        return manifest
    full_df: pd.DataFrame | None = None

    def get_full_df() -> pd.DataFrame:
        nonlocal full_df
        if full_df is None:
            full_df = load_analysis_data(parquet_path)
        return full_df

    incremental_info: dict[str, Any] = {
        "mode": "full",
    }

    if incremental_country_enabled:
        print("📊 增量更新国家汇总（按变化分区键）...")
        country_summary, incremental_info = (
            compute_country_summary_incremental(
                partitioned_dataset_path=partitioned_dataset_path,
                output_dir=country_summary_source_dir,
                changed_country_keys=changed_keys,
            )
        )
        print(f"   ✓ 国家汇总已更新，共 {len(country_summary)} 行")
    else:
        print("📊 计算国家汇总（全量）...")
        country_summary = compute_country_summary(get_full_df())
        print(f"   ✓ {len(country_summary)} 个国家")

    # 第一阶段优先减少刷新开销：非国家汇总优先复用已有结果。
    year_month_summary = load_existing_summary(
        output_dir,
        "yearMonth",
    )
    powertrain_summary = load_existing_summary(
        output_dir,
        "powertrain",
    )
    segment_summary = load_existing_summary(
        output_dir,
        "segment",
    )
    top_makes_summary = load_existing_summary(
        output_dir,
        "topMakes",
    )

    missing_non_country = (
        year_month_summary.empty
        or powertrain_summary.empty
        or segment_summary.empty
        or top_makes_summary.empty
    )
    if missing_non_country:
        print("📊 检测到非国家汇总缺失，执行一次全量兜底计算...")
        df = get_full_df()
        if year_month_summary.empty:
            year_month_summary = compute_year_month_summary(df)
        if powertrain_summary.empty:
            powertrain_summary = compute_powertrain_summary(df)
        if segment_summary.empty:
            segment_summary = compute_segment_summary(df)
        if top_makes_summary.empty:
            top_makes_summary = compute_top_makes_summary(df, top_n=20)
        del df

    # Keep exactly one full DataFrame alive and release it before serializing the
    # compact outputs. This is important when the isolated worker has a hard
    # address-space limit.
    full_df = None

    summaries = {
        "country": country_summary,
        "yearMonth": year_month_summary,
        "powertrain": powertrain_summary,
        "segment": segment_summary,
        "topMakes": top_makes_summary,
    }

    print(f"\n💾 保存预聚合表到 {output_dir}...")
    saved_files = save_summary_tables(summaries, output_dir)

    precompute_mode = str(incremental_info.get("mode", "full"))
    manifest = generate_summaries_manifest(
        saved_files=saved_files,
        original_row_count=original_rows,
        precompute_mode=precompute_mode,
        incremental_info=incremental_info,
    )

    manifest_path = Path(output_dir) / "summaries_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"   ✓ 清单已保存: {manifest_path}")
    print(
        f"\n📈 汇总：原始 {original_rows} 行 -> "
        f"汇总 {manifest['totalSummaryRows']} 行"
    )
    print(f"📈 带宽降低：{manifest['bandwidthReduction']}")

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Precompute dashboard summaries from one parquet candidate."
    )
    parser.add_argument(
        "--parquet",
        default=None,
        help="Input parquet file or dataset directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Destination directory for summary parquet/csv files.",
    )
    args = parser.parse_args()
    project_root = get_project_root()
    parquet_path = args.parquet or str(
        project_root / "04_Processed_data" / "fullParquetV1.parquet"
    )
    output_dir = args.output_dir or str(
        project_root / "04_Processed_data" / "summaries"
    )

    manifest = precompute_all_summaries(parquet_path, output_dir)
    print(
        f"\n✅ 预聚合完成！清单中有 {len(manifest['summaries'])} 个汇总表"
    )
