import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pandas as pd

from logging_utils import build_job_id, get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_FILE = (
    PROJECT_ROOT / "04_Processed_data/jato_full_archive.parquet"
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "04_Processed_data/partitioned_dataset_v1"
MANIFEST_SCHEMA_VERSION = "1.1"
SUPPORTED_MANIFEST_SCHEMA_VERSIONS = {"1.0", "1.1"}


def read_json_if_exists(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def to_project_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def resolve_path(path_text: str, default_path: Path) -> Path:
    if not path_text:
        return default_path

    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def find_column(
    column_map: dict[str, str],
    candidates: list[str],
) -> str | None:
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in column_map:
            return column_map[key]
    return None


def resolve_partition_columns(
    df: pd.DataFrame,
    requested_columns: list[str],
) -> list[str]:
    column_map = {
        str(column).strip().lower(): str(column)
        for column in df.columns
    }

    resolved: list[str] = []
    for item in requested_columns:
        name = item.strip()
        if not name:
            continue

        target = find_column(column_map, [name])
        if not target and name.lower() in ["国家", "country"]:
            target = find_column(column_map, ["国家", "Country", "country"])

        if not target:
            raise ValueError(f"分区列不存在: {name}")

        if target not in resolved:
            resolved.append(target)

    if not resolved:
        raise ValueError("未解析到任何有效分区列。")

    return resolved


def normalize_partition_values(
    df: pd.DataFrame,
    partition_columns: list[str],
) -> pd.DataFrame:
    result = df.copy()
    for column in partition_columns:
        series = result[column].astype("string").fillna("未标注")
        result[column] = series.str.strip().replace("", "未标注")
    return result


def normalize_partition_token(value: object) -> str:
    if value is None:
        return "未标注"
    text = str(value).strip()
    return text if text else "未标注"


def key_to_tuple(
    key: object,
    partition_columns: list[str],
) -> tuple[str, ...]:
    if len(partition_columns) == 1:
        if isinstance(key, tuple):
            if len(key) != 1:
                raise ValueError("单分区列场景下 group key 维度异常。")
            key = key[0]
        return (normalize_partition_token(key),)

    if not isinstance(key, tuple):
        raise ValueError("多分区列场景下 group key 非 tuple。")

    return tuple(normalize_partition_token(value) for value in key)


def build_partition_dir(
    partition_columns: list[str],
    key_values: tuple[str, ...],
) -> str:
    parts = []
    for column, value in zip(partition_columns, key_values, strict=False):
        parts.append(f"{column}={quote(value, safe='')}")
    return "/".join(parts)


def compute_partition_signature(partition_df: pd.DataFrame) -> str:
    hash_values = pd.util.hash_pandas_object(
        partition_df,
        index=False,
        categorize=True,
    )
    checksum = int(hash_values.astype("uint64").sum())
    return str(checksum)


def build_partition_stats(
    df: pd.DataFrame,
    partition_columns: list[str],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}

    grouped = df.groupby(partition_columns, dropna=False, sort=False)
    for key, group_df in grouped:
        key_values = key_to_tuple(key, partition_columns)
        partition_dir = build_partition_dir(partition_columns, key_values)
        payload_df = group_df.drop(columns=partition_columns)
        stats[partition_dir] = {
            "rows": int(len(group_df)),
            "signature": compute_partition_signature(payload_df),
        }

    return stats


def extract_partition_stats(
    manifest_payload: dict | None,
) -> dict[str, dict[str, object]]:
    if not manifest_payload:
        return {}

    raw_stats = manifest_payload.get("partitionStats")
    if not isinstance(raw_stats, dict):
        return {}

    normalized: dict[str, dict[str, object]] = {}
    for key, value in raw_stats.items():
        if not isinstance(value, dict):
            continue
        rows = int(value.get("rows", 0))
        signature = str(value.get("signature", ""))
        normalized[str(key)] = {
            "rows": rows,
            "signature": signature,
        }
    return normalized


def plan_incremental_changes(
    previous_stats: dict[str, dict[str, object]],
    current_stats: dict[str, dict[str, object]],
) -> tuple[list[str], list[str], list[str]]:
    previous_dirs = set(previous_stats)
    current_dirs = set(current_stats)

    added_dirs = sorted(current_dirs - previous_dirs)
    removed_dirs = sorted(previous_dirs - current_dirs)

    updated_dirs: list[str] = []
    for partition_dir in sorted(previous_dirs & current_dirs):
        previous = previous_stats.get(partition_dir, {})
        current = current_stats.get(partition_dir, {})
        if (
            int(previous.get("rows", 0)) != int(current.get("rows", 0))
            or str(previous.get("signature", ""))
            != str(current.get("signature", ""))
        ):
            updated_dirs.append(partition_dir)

    return added_dirs, updated_dirs, removed_dirs


def write_single_partition_file(
    partition_df: pd.DataFrame,
    partition_columns: list[str],
    partition_path: Path,
) -> None:
    partition_payload = partition_df.drop(columns=partition_columns)
    partition_path.mkdir(parents=True, exist_ok=True)
    output_file = partition_path / "part-00000.parquet"
    partition_payload.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )


def apply_incremental_partition_update(
    df: pd.DataFrame,
    partition_columns: list[str],
    target_dir: Path,
    rewrite_dirs: set[str],
    removed_dirs: list[str],
) -> dict[str, int]:
    for partition_dir in removed_dirs:
        path = target_dir / partition_dir
        if path.exists():
            shutil.rmtree(path)

    grouped = df.groupby(partition_columns, dropna=False, sort=False)
    rewritten_count = 0
    for key, group_df in grouped:
        key_values = key_to_tuple(key, partition_columns)
        partition_dir = build_partition_dir(partition_columns, key_values)
        if partition_dir not in rewrite_dirs:
            continue

        partition_path = target_dir / partition_dir
        if partition_path.exists():
            shutil.rmtree(partition_path)
        write_single_partition_file(
            partition_df=group_df,
            partition_columns=partition_columns,
            partition_path=partition_path,
        )
        rewritten_count += 1

    return {
        "rewrittenPartitions": int(rewritten_count),
        "removedPartitions": int(len(removed_dirs)),
    }


def validate_source_manifest_schema(source_file: Path) -> str | None:
    manifest_path = source_file.parent / "manifest.json"
    if not manifest_path.exists():
        return None

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ValueError(
            f"源 manifest 解析失败: {manifest_path}"
        ) from error

    schema_version = str(payload.get("manifestSchemaVersion", "1.0"))
    if schema_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        supported = ", ".join(sorted(SUPPORTED_MANIFEST_SCHEMA_VERSIONS))
        raise ValueError(
            f"源 manifest schema 不兼容: {schema_version}（支持: {supported}）"
        )

    return schema_version


def evaluate_partition_output(
    df: pd.DataFrame,
    partition_columns: list[str],
) -> dict[str, object]:
    if df.empty:
        raise ValueError("输入数据为空，无法构建分区数据集。")

    partition_cardinality: dict[str, int] = {}
    for column in partition_columns:
        partition_cardinality[column] = int(
            df[column].astype("string").nunique(dropna=True)
        )

    return {
        "inputRows": int(len(df)),
        "inputColumns": int(len(df.columns)),
        "partitionCardinality": partition_cardinality,
    }


def write_manifest(
    output_dir: Path,
    source_file: Path,
    partition_columns: list[str],
    row_count: int,
    column_count: int,
    elapsed_seconds: float,
    validation_summary: dict[str, object],
    source_manifest_schema_version: str | None,
    partition_stats: dict[str, dict[str, object]],
    update_summary: dict[str, object],
) -> Path:
    parquet_files = sorted(output_dir.rglob("*.parquet"))
    total_bytes = sum(file.stat().st_size for file in parquet_files)
    partition_dirs = sorted(
        {
            str(file.parent.relative_to(output_dir))
            for file in parquet_files
        }
    )

    manifest = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "manifestSchemaVersion": MANIFEST_SCHEMA_VERSION,
        "pipelineVersion": "1.0",
        "sourceParquet": to_project_relative(source_file),
        "sourceManifestSchemaVersion": source_manifest_schema_version,
        "outputDatasetDir": to_project_relative(output_dir),
        "partitionColumns": partition_columns,
        "rows": int(row_count),
        "columns": int(column_count),
        "parquetFileCount": int(len(parquet_files)),
        "partitionDirectoryCount": int(len(partition_dirs)),
        "partitionDirectories": partition_dirs,
        "partitionStats": partition_stats,
        "parquetTotalBytes": int(total_bytes),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "validationSummary": validation_summary,
        "partitionUpdateSummary": update_summary,
    }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def build_partitioned_dataset(
    input_path: str,
    output_dir: str,
    partition_cols: list[str],
    overwrite: bool,
    incremental: bool = False,
    job_id: str | None = None,
) -> tuple[Path, Path]:
    logger = get_logger("jato.partition", job_id=job_id)

    def emit(message: str) -> None:
        print(message)
        logger.info(message)

    source_file = resolve_path(input_path, DEFAULT_INPUT_FILE)
    target_dir = resolve_path(output_dir, DEFAULT_OUTPUT_DIR)

    if not source_file.exists():
        raise FileNotFoundError(f"输入 Parquet 不存在: {source_file}")

    source_manifest_schema_version = validate_source_manifest_schema(
        source_file
    )

    existing_manifest_path = target_dir / "manifest.json"
    previous_manifest_payload = read_json_if_exists(existing_manifest_path)
    previous_partition_stats = extract_partition_stats(
        previous_manifest_payload
    )

    has_existing_dataset = (
        target_dir.exists() and any(target_dir.rglob("*.parquet"))
    )

    if has_existing_dataset and not overwrite and not incremental:
        raise ValueError(
            "输出目录已有分区数据，请使用 --overwrite 或 --incremental。"
        )

    if overwrite and target_dir.exists():
        shutil.rmtree(target_dir)

    baseline_rebuild = False
    if incremental and has_existing_dataset and not previous_partition_stats:
        emit("♻️ 未发现历史分区签名，执行一次基线重建。")
        shutil.rmtree(target_dir)
        baseline_rebuild = True
        has_existing_dataset = False

    target_dir.mkdir(parents=True, exist_ok=True)

    emit(f"🚀 开始构建分区数据: {to_project_relative(source_file)}")
    start_time = time.time()

    df = pd.read_parquet(source_file)
    df.columns = [str(column).strip() for column in df.columns]

    partition_columns = resolve_partition_columns(df, partition_cols)
    df = normalize_partition_values(df, partition_columns)
    validation_summary = evaluate_partition_output(df, partition_columns)
    current_partition_stats = build_partition_stats(df, partition_columns)

    emit(f"🧩 分区列: {partition_columns}")
    emit(f"💾 输出目录: {to_project_relative(target_dir)}")
    if source_manifest_schema_version:
        emit(f"🧾 源 manifest schema: {source_manifest_schema_version}")
    else:
        emit("🧾 源 manifest schema: 未找到（按 legacy 处理）")

    update_summary: dict[str, object]
    if incremental and has_existing_dataset and previous_partition_stats:
        added_dirs, updated_dirs, removed_dirs = plan_incremental_changes(
            previous_stats=previous_partition_stats,
            current_stats=current_partition_stats,
        )
        rewrite_dirs = set(added_dirs + updated_dirs)

        apply_result = apply_incremental_partition_update(
            df=df,
            partition_columns=partition_columns,
            target_dir=target_dir,
            rewrite_dirs=rewrite_dirs,
            removed_dirs=removed_dirs,
        )
        update_summary = {
            "mode": "incremental",
            "addedPartitions": int(len(added_dirs)),
            "updatedPartitions": int(len(updated_dirs)),
            "removedPartitions": int(len(removed_dirs)),
            "rewrittenPartitions": int(apply_result["rewrittenPartitions"]),
        }
        emit(
            "♻️ 增量更新: "
            f"新增 {len(added_dirs)}，更新 {len(updated_dirs)}，"
            f"删除 {len(removed_dirs)}"
        )
    else:
        df.to_parquet(
            target_dir,
            engine="pyarrow",
            compression="snappy",
            index=False,
            partition_cols=partition_columns,
        )
        mode = "full_rebuild"
        if baseline_rebuild:
            mode = "baseline_rebuild"
        update_summary = {
            "mode": mode,
            "addedPartitions": int(len(current_partition_stats)),
            "updatedPartitions": 0,
            "removedPartitions": 0,
            "rewrittenPartitions": int(len(current_partition_stats)),
        }

    elapsed = time.time() - start_time
    manifest_path = write_manifest(
        output_dir=target_dir,
        source_file=source_file,
        partition_columns=partition_columns,
        row_count=len(df),
        column_count=len(df.columns),
        elapsed_seconds=elapsed,
        validation_summary=validation_summary,
        source_manifest_schema_version=source_manifest_schema_version,
        partition_stats=current_partition_stats,
        update_summary=update_summary,
    )

    emit("✅ 分区构建完成")
    emit(f"📦 目录: {to_project_relative(target_dir)}")
    emit(f"🧾 Manifest: {to_project_relative(manifest_path)}")
    emit(f"⏱️ 总耗时: {elapsed:.2f} 秒")
    return target_dir, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将全量 Parquet 产物构建为分区数据集。",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_INPUT_FILE),
        help="输入 Parquet 路径。",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="输出分区目录。",
    )
    parser.add_argument(
        "--partition-cols",
        type=str,
        default="国家",
        help="分区列，多个列用逗号分隔（示例：国家,细分市场（按车长））。",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="若输出目录存在则先清空。",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="仅重写变化分区（需已存在分区数据和 manifest）。",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="日志作业 ID（不传则自动生成）。",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_id = args.job_id or build_job_id("partition")
    logger = get_logger("jato.partition", job_id=job_id)
    partition_cols = [
        value.strip()
        for value in args.partition_cols.split(",")
        if value.strip()
    ]

    try:
        build_partitioned_dataset(
            input_path=args.input,
            output_dir=args.output_dir,
            partition_cols=partition_cols,
            overwrite=args.overwrite,
            incremental=args.incremental,
            job_id=job_id,
        )
    except Exception as error:
        logger.error("分区构建失败[%s] %s", type(error).__name__, error)
        print(
            f"❌ 分区构建失败[{type(error).__name__}] {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
