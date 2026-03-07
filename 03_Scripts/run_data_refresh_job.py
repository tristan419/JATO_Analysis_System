import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from build_partitioned_dataset import (
    SUPPORTED_MANIFEST_SCHEMA_VERSIONS,
    build_partitioned_dataset,
)
from elt_worker import convert_jato_to_parquet, resolve_input_file
from logging_utils import build_job_id, get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FINGERPRINT_FILE = "04_Processed_data/dataset_fingerprint.json"
FINGERPRINT_SCHEMA_VERSION = "1.0"


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def to_project_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ValueError(f"JSON 解析失败: {path}") from error


def read_json_if_exists(path: Path) -> dict | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_refresh_fingerprint(
    source_file: Path,
    sheet_name: str,
    partition_cols: list[str],
) -> dict[str, Any]:
    source_stat = source_file.stat()
    return {
        "schemaVersion": FINGERPRINT_SCHEMA_VERSION,
        "sourceFile": to_project_relative(source_file),
        "sourceFileBytes": int(source_stat.st_size),
        "sourceFileMtimeNs": int(source_stat.st_mtime_ns),
        "sheetName": str(sheet_name),
        "partitionCols": list(partition_cols),
    }


def should_skip_for_unchanged(
    fingerprint_path: Path,
    current_fingerprint: dict[str, Any],
) -> tuple[bool, dict[str, Any] | None]:
    previous_fingerprint = read_json_if_exists(fingerprint_path)
    if not previous_fingerprint:
        return False, None
    return previous_fingerprint == current_fingerprint, previous_fingerprint


def resolve_existing_output_paths(
    output_path: str,
    manifest_path: str,
    partition_output: str,
) -> tuple[Path, Path, Path, Path]:
    full_parquet_path = resolve_path(output_path)
    full_manifest_path = resolve_path(manifest_path)
    partition_dir = resolve_path(partition_output)
    partition_manifest_path = partition_dir / "manifest.json"
    return (
        full_parquet_path,
        full_manifest_path,
        partition_dir,
        partition_manifest_path,
    )


def write_report(report_path_text: str, report: dict) -> Path:
    report_path = resolve_path(report_path_text)
    write_json(report_path, report)
    return report_path


def validate_manifests(
    full_manifest: dict,
    partition_manifest: dict,
) -> None:
    full_rows = int(full_manifest.get("rows", 0))
    partition_rows = int(partition_manifest.get("rows", 0))

    if full_rows <= 0:
        raise ValueError("全量 manifest 行数无效。")
    if partition_rows <= 0:
        raise ValueError("分区 manifest 行数无效。")
    if full_rows != partition_rows:
        raise ValueError(
            f"行数不一致：全量={full_rows}, 分区={partition_rows}"
        )

    schema_version = str(
        partition_manifest.get("manifestSchemaVersion", "1.0")
    )
    if schema_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        supported = ", ".join(sorted(SUPPORTED_MANIFEST_SCHEMA_VERSIONS))
        raise ValueError(
            f"分区 manifest schema 不兼容: {schema_version}（支持: {supported}）"
        )


def extract_partition_directories(
    partition_manifest: dict | None,
) -> set[str]:
    if not partition_manifest:
        return set()

    raw_dirs = partition_manifest.get("partitionDirectories", [])
    if not isinstance(raw_dirs, list):
        return set()

    return {
        str(item)
        for item in raw_dirs
        if str(item).strip()
    }


def extract_partition_stats(
    partition_manifest: dict | None,
) -> dict[str, dict[str, object]]:
    if not partition_manifest:
        return {}

    raw_stats = partition_manifest.get("partitionStats", {})
    if not isinstance(raw_stats, dict):
        return {}

    normalized: dict[str, dict[str, object]] = {}
    for key, value in raw_stats.items():
        if not isinstance(value, dict):
            continue
        normalized[str(key)] = {
            "rows": int(value.get("rows", 0)),
            "signature": str(value.get("signature", "")),
        }
    return normalized


def build_incremental_regression_summary(
    previous_full_manifest: dict | None,
    previous_partition_manifest: dict | None,
    current_full_manifest: dict,
    current_partition_manifest: dict,
) -> dict[str, Any]:
    current_rows = int(current_full_manifest.get("rows", 0))
    previous_rows = (
        int(previous_full_manifest.get("rows", 0))
        if previous_full_manifest
        else None
    )

    if previous_rows is None:
        row_delta = current_rows
        changed_rows = current_rows
    else:
        row_delta = current_rows - previous_rows
        changed_rows = abs(row_delta)

    current_partition_count = int(
        current_partition_manifest.get("partitionDirectoryCount", 0)
    )
    previous_partition_count = (
        int(previous_partition_manifest.get("partitionDirectoryCount", 0))
        if previous_partition_manifest
        else None
    )

    previous_dirs = extract_partition_directories(previous_partition_manifest)
    current_dirs = extract_partition_directories(current_partition_manifest)
    previous_stats = extract_partition_stats(previous_partition_manifest)
    current_stats = extract_partition_stats(current_partition_manifest)

    if previous_stats or current_stats:
        previous_dirs = set(previous_stats)
        current_dirs = set(current_stats)

        added_dirs = sorted(current_dirs - previous_dirs)
        removed_dirs = sorted(previous_dirs - current_dirs)

        updated_dirs: list[str] = []
        for partition_dir in sorted(previous_dirs & current_dirs):
            previous_payload = previous_stats.get(partition_dir, {})
            current_payload = current_stats.get(partition_dir, {})
            if (
                int(previous_payload.get("rows", 0))
                != int(current_payload.get("rows", 0))
                or str(previous_payload.get("signature", ""))
                != str(current_payload.get("signature", ""))
            ):
                updated_dirs.append(partition_dir)

        changed_partition_count = (
            len(added_dirs) + len(removed_dirs) + len(updated_dirs)
        )
    elif previous_dirs or current_dirs:
        added_dirs = sorted(current_dirs - previous_dirs)
        removed_dirs = sorted(previous_dirs - current_dirs)
        updated_dirs = []
        changed_partition_count = len(added_dirs) + len(removed_dirs)
    elif previous_partition_count is None:
        added_dirs = []
        removed_dirs = []
        updated_dirs = []
        changed_partition_count = current_partition_count
    else:
        added_dirs = []
        removed_dirs = []
        updated_dirs = []
        changed_partition_count = abs(
            current_partition_count - previous_partition_count
        )

    return {
        "previousRows": previous_rows,
        "currentRows": current_rows,
        "rowDelta": int(row_delta),
        "changedRows": int(changed_rows),
        "previousPartitionCount": previous_partition_count,
        "currentPartitionCount": current_partition_count,
        "changedPartitionCount": int(changed_partition_count),
        "addedPartitionDirectories": added_dirs,
        "updatedPartitionDirectories": updated_dirs,
        "removedPartitionDirectories": removed_dirs,
    }


def run_refresh_job(args: argparse.Namespace) -> dict:
    job_start = time.time()
    job_id = args.job_id or build_job_id("refresh")
    logger = get_logger("jato.refresh", job_id=job_id)

    def emit(message: str) -> None:
        print(message)
        logger.info(message)

    partition_cols = [
        value.strip()
        for value in args.partition_cols.split(",")
        if value.strip()
    ]
    if not partition_cols:
        raise ValueError("至少需要一个分区列。")

    logger.info(
        "刷新任务启动: incremental=%s skipUnchanged=%s skipBenchmark=%s",
        bool(args.incremental),
        bool(args.skip_unchanged),
        bool(args.skip_benchmark),
    )

    incremental_enabled = bool(args.incremental or args.skip_unchanged)
    source_input_file = resolve_input_file(args.input, args.raw_dir)
    fingerprint_path = resolve_path(args.fingerprint)
    current_fingerprint = build_refresh_fingerprint(
        source_file=source_input_file,
        sheet_name=args.sheet,
        partition_cols=partition_cols,
    )

    (
        _,
        existing_full_manifest_path,
        _,
        existing_partition_manifest_path,
    ) = resolve_existing_output_paths(
        output_path=args.output,
        manifest_path=args.manifest,
        partition_output=args.partition_output,
    )
    previous_full_manifest_payload = read_json_if_exists(
        existing_full_manifest_path
    )
    previous_partition_manifest_payload = read_json_if_exists(
        existing_partition_manifest_path
    )

    step_durations: dict[str, float] = {}

    fingerprint_matched = False
    if incremental_enabled and args.skip_unchanged:
        fingerprint_matched, _ = should_skip_for_unchanged(
            fingerprint_path=fingerprint_path,
            current_fingerprint=current_fingerprint,
        )

    if fingerprint_matched:
        (
            full_parquet_path,
            full_manifest_path,
            partition_dir,
            partition_manifest_path,
        ) = resolve_existing_output_paths(
            output_path=args.output,
            manifest_path=args.manifest,
            partition_output=args.partition_output,
        )

        if (
            not full_manifest_path.exists()
            or not partition_manifest_path.exists()
        ):
            raise ValueError(
                "命中 skip-unchanged，但现有 manifest 缺失；"
                "请先执行一次完整刷新。"
            )

        full_manifest_payload = read_json(full_manifest_path)
        partition_manifest_payload = read_json(partition_manifest_path)
        validate_manifests(full_manifest_payload, partition_manifest_payload)

        report = {
            "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
            "jobId": job_id,
            "jobStatus": "skipped_unchanged",
            "input": {
                "excelInput": args.input,
                "resolvedExcelInput": to_project_relative(source_input_file),
                "rawDir": args.raw_dir,
                "sheet": args.sheet,
                "partitionCols": partition_cols,
            },
            "outputs": {
                "fullParquet": to_project_relative(full_parquet_path),
                "fullManifest": to_project_relative(full_manifest_path),
                "partitionDataset": to_project_relative(partition_dir),
                "partitionManifest": to_project_relative(
                    partition_manifest_path
                ),
            },
            "stepDurations": step_durations,
            "fullManifest": {
                "rows": int(full_manifest_payload.get("rows", 0)),
                "columns": int(full_manifest_payload.get("columns", 0)),
                "schemaVersion": str(
                    full_manifest_payload.get("manifestSchemaVersion", "1.0")
                ),
            },
            "partitionManifest": {
                "rows": int(partition_manifest_payload.get("rows", 0)),
                "columns": int(partition_manifest_payload.get("columns", 0)),
                "schemaVersion": str(
                    partition_manifest_payload.get(
                        "manifestSchemaVersion",
                        "1.0",
                    )
                ),
                "parquetFileCount": int(
                    partition_manifest_payload.get("parquetFileCount", 0)
                ),
            },
            "benchmark": None,
            "incremental": {
                "enabled": incremental_enabled,
                "skipUnchanged": bool(args.skip_unchanged),
                "fingerprintPath": to_project_relative(fingerprint_path),
                "fingerprintMatched": True,
                "fingerprintUpdated": False,
                "regression": build_incremental_regression_summary(
                    previous_full_manifest=previous_full_manifest_payload,
                    previous_partition_manifest=(
                        previous_partition_manifest_payload
                    ),
                    current_full_manifest=full_manifest_payload,
                    current_partition_manifest=partition_manifest_payload,
                ),
            },
            "jobElapsedSeconds": round(time.time() - job_start, 3),
        }
        report_path = write_report(args.report, report)
        emit("ℹ️ 输入未变化，已跳过 ETL/分区重建。")
        emit(f"📄 报告: {to_project_relative(report_path)}")
        return report

    step_start = time.time()
    output_parquet, output_manifest = convert_jato_to_parquet(
        input_path=args.input,
        raw_dir=args.raw_dir,
        output_path=args.output,
        manifest_path=args.manifest,
        sheet_name=args.sheet,
        job_id=job_id,
    )
    step_durations["etlSeconds"] = round(time.time() - step_start, 3)

    step_start = time.time()
    effective_partition_overwrite = bool(
        args.overwrite_partition or not incremental_enabled
    )
    partition_dir, partition_manifest = build_partitioned_dataset(
        input_path=str(output_parquet),
        output_dir=args.partition_output,
        partition_cols=partition_cols,
        overwrite=effective_partition_overwrite,
        incremental=incremental_enabled,
        job_id=job_id,
    )
    step_durations["partitionSeconds"] = round(
        time.time() - step_start,
        3,
    )

    full_manifest_payload = read_json(output_manifest)
    partition_manifest_payload = read_json(partition_manifest)
    validate_manifests(full_manifest_payload, partition_manifest_payload)

    benchmark_summary = None
    if not args.skip_benchmark:
        from benchmark_dashboard_load import collect_benchmark

        step_start = time.time()
        benchmark_summary = collect_benchmark(args.benchmark_repeats)
        step_durations["benchmarkSeconds"] = round(
            time.time() - step_start,
            3,
        )

    report = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "jobId": job_id,
        "jobStatus": "success",
        "input": {
            "excelInput": args.input,
            "resolvedExcelInput": to_project_relative(source_input_file),
            "rawDir": args.raw_dir,
            "sheet": args.sheet,
            "partitionCols": partition_cols,
            "partitionOverwrite": effective_partition_overwrite,
        },
        "outputs": {
            "fullParquet": to_project_relative(output_parquet),
            "fullManifest": to_project_relative(output_manifest),
            "partitionDataset": to_project_relative(partition_dir),
            "partitionManifest": to_project_relative(partition_manifest),
        },
        "stepDurations": step_durations,
        "fullManifest": {
            "rows": int(full_manifest_payload.get("rows", 0)),
            "columns": int(full_manifest_payload.get("columns", 0)),
            "schemaVersion": str(
                full_manifest_payload.get("manifestSchemaVersion", "1.0")
            ),
        },
        "partitionManifest": {
            "rows": int(partition_manifest_payload.get("rows", 0)),
            "columns": int(partition_manifest_payload.get("columns", 0)),
            "schemaVersion": str(
                partition_manifest_payload.get(
                    "manifestSchemaVersion",
                    "1.0",
                )
            ),
            "parquetFileCount": int(
                partition_manifest_payload.get("parquetFileCount", 0)
            ),
        },
        "benchmark": benchmark_summary,
        "incremental": {
            "enabled": incremental_enabled,
            "skipUnchanged": bool(args.skip_unchanged),
            "fingerprintPath": to_project_relative(fingerprint_path),
            "fingerprintMatched": False,
            "fingerprintUpdated": incremental_enabled,
            "regression": build_incremental_regression_summary(
                previous_full_manifest=previous_full_manifest_payload,
                previous_partition_manifest=(
                    previous_partition_manifest_payload
                ),
                current_full_manifest=full_manifest_payload,
                current_partition_manifest=partition_manifest_payload,
            ),
        },
        "jobElapsedSeconds": round(time.time() - job_start, 3),
    }

    if incremental_enabled:
        write_json(fingerprint_path, current_fingerprint)

    report_path = write_report(args.report, report)

    emit("✅ 数据刷新作业完成")
    emit(f"📄 报告: {to_project_relative(report_path)}")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="一键执行数据刷新：ETL -> 分区 -> 校验 -> 基准。",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="原始 Excel 文件路径（可选）。",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default="01_RAW_DATA",
        help="RawData 目录（未传 --input 时使用）。",
    )
    parser.add_argument(
        "--sheet",
        type=str,
        default="Data Export",
        help="Excel Sheet 名称。",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="04_Processed_data/jato_full_archive.parquet",
        help="全量 Parquet 输出路径。",
    )
    parser.add_argument(
        "--manifest",
        type=str,
        default="04_Processed_data/manifest.json",
        help="全量 manifest 输出路径。",
    )
    parser.add_argument(
        "--partition-output",
        type=str,
        default="04_Processed_data/partitioned_dataset_v1",
        help="分区数据输出目录。",
    )
    parser.add_argument(
        "--partition-cols",
        type=str,
        default="国家",
        help="分区列，多个使用逗号分隔。",
    )
    parser.add_argument(
        "--overwrite-partition",
        action="store_true",
        help="重建分区目录（存在则先清空）。",
    )
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="跳过 dashboard 基准测试。",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="启用增量模式（记录输入指纹，用于后续变更检测）。",
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="若输入指纹未变化则跳过 ETL 与分区重建。",
    )
    parser.add_argument(
        "--fingerprint",
        type=str,
        default=DEFAULT_FINGERPRINT_FILE,
        help="输入指纹文件路径（用于增量变更检测）。",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="日志作业 ID（不传则自动生成）。",
    )
    parser.add_argument(
        "--benchmark-repeats",
        type=int,
        default=2,
        help="基准测试重复次数（默认 2）。",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="04_Processed_data/refresh_job_report.json",
        help="作业报告输出路径。",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_id = args.job_id or build_job_id("refresh")
    logger = get_logger("jato.refresh", job_id=job_id)
    args.job_id = job_id
    try:
        run_refresh_job(args)
    except Exception as error:
        logger.error("数据刷新作业失败[%s] %s", type(error).__name__, error)
        print(
            f"❌ 数据刷新作业失败[{type(error).__name__}] {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
