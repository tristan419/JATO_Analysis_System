import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from logging_utils import build_job_id, get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = PROJECT_ROOT / "01_RAW_DATA"
DEFAULT_OUTPUT_FILE = (
    PROJECT_ROOT / "04_Processed_data/jato_full_archive.parquet"
)
DEFAULT_SHEET = "Data Export"
MANIFEST_SCHEMA_VERSION = "1.1"
SUPPORTED_CONFLICT_POLICIES = {"report_only", "fail", "last_wins"}
SOURCE_TRACK_COLUMNS = (
    "__source_file",
    "__source_index",
    "__source_row",
)

YEAR_COL_PATTERN = re.compile(r"^\d{4}$")
MONTH_COL_PATTERN = re.compile(
    r"^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)

RECOMMENDED_DIMENSION_CANDIDATES: dict[str, tuple[str, ...]] = {
    "country": ("国家", "country"),
    "segment": ("细分市场（按车长）", "细分市场", "segment"),
    "powertrain": ("动总规整", "powertrain"),
    "make": ("make", "品牌"),
    "model": ("model",),
    "version": ("version name", "versionname"),
}


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_raw_dir(raw_dir: str) -> Path:
    raw_dir_path = Path(raw_dir)
    if not raw_dir_path.is_absolute():
        raw_dir_path = PROJECT_ROOT / raw_dir_path
    if not raw_dir_path.exists():
        raise FileNotFoundError(f"RawData 目录不存在: {raw_dir_path}")
    return raw_dir_path


def list_xlsx_candidates(raw_dir: Path) -> list[Path]:
    candidates = [
        path
        for path in raw_dir.glob("*.xlsx")
        if not path.name.startswith("~$")
    ]
    if not candidates:
        raise FileNotFoundError(f"未在目录中找到 Excel 文件: {raw_dir}")
    return sorted(candidates, key=lambda path: (path.stat().st_mtime_ns, path.name))


def resolve_explicit_file(path_text: str) -> Path:
    candidate = Path(path_text)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if not candidate.exists():
        raise FileNotFoundError(f"输入文件不存在: {candidate}")
    return candidate


def find_latest_xlsx(raw_dir: Path) -> Path:
    candidates = list_xlsx_candidates(raw_dir)
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def resolve_input_file(input_path: str | None, raw_dir: str) -> Path:
    if input_path:
        return resolve_explicit_file(input_path)

    raw_dir_path = resolve_raw_dir(raw_dir)

    preferred = raw_dir_path / "JATO-2026.1.xlsx"
    if preferred.exists():
        return preferred

    return find_latest_xlsx(raw_dir_path)


def resolve_input_files(
    input_path: str | None,
    input_files: str | None,
    raw_dir: str,
    merge_all_xlsx: bool,
) -> list[Path]:
    file_list = parse_csv_list(input_files)
    selector_count = (
        int(bool(input_path))
        + int(bool(file_list))
        + int(bool(merge_all_xlsx))
    )
    if selector_count > 1:
        raise ValueError(
            "--input / --input-files / --merge-all-xlsx 只能三选一。"
        )

    if file_list:
        return [resolve_explicit_file(path_text) for path_text in file_list]

    if merge_all_xlsx:
        return list_xlsx_candidates(resolve_raw_dir(raw_dir))

    return [resolve_input_file(input_path, raw_dir)]


def read_excel_with_fallback(
    input_file: Path,
    sheet_name: str,
) -> pd.DataFrame:
    try:
        return pd.read_excel(
            input_file,
            sheet_name=sheet_name,
            engine="calamine",
        )
    except Exception:
        try:
            return pd.read_excel(
                input_file,
                sheet_name=sheet_name,
            )
        except Exception as default_error:
            raise RuntimeError(
                "读取 Excel 失败：calamine 与默认引擎均不可用。"
            ) from default_error


def add_source_tracking_columns(
    df: pd.DataFrame,
    source_file: Path,
    source_index: int,
) -> pd.DataFrame:
    tracked = df.copy()
    tracked[SOURCE_TRACK_COLUMNS[0]] = str(to_project_relative(source_file))
    tracked[SOURCE_TRACK_COLUMNS[1]] = int(source_index)
    tracked[SOURCE_TRACK_COLUMNS[2]] = range(1, len(tracked) + 1)
    return tracked


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip() for column in result.columns]

    object_cols = result.select_dtypes(include=["object"]).columns
    for column in object_cols:
        result[column] = result[column].astype("string")

    return result


def _to_key_dict(
    key_values: object,
    key_columns: list[str],
) -> dict[str, str]:
    if len(key_columns) == 1 and not isinstance(key_values, tuple):
        values = (key_values,)
    elif isinstance(key_values, tuple):
        values = key_values
    else:
        values = (key_values,)

    payload: dict[str, str] = {}
    for column, value in zip(key_columns, values, strict=False):
        if pd.isna(value):
            payload[column] = ""
        else:
            payload[column] = str(value)
    return payload


def detect_cross_file_conflicts(
    df: pd.DataFrame,
    conflict_keys: list[str],
) -> dict[str, Any]:
    if not conflict_keys:
        return {
            "enabled": False,
            "policy": "disabled",
            "requestedConflictKeys": [],
            "resolvedConflictKeys": [],
            "hasConflicts": False,
            "conflictGroupCount": 0,
            "conflictRowCount": 0,
            "sampleConflicts": [],
            "sampleLimit": 20,
        }

    resolved_keys = resolve_columns_case_insensitive(
        columns=[str(column) for column in df.columns],
        requested=conflict_keys,
    )

    payload_excluded = set(resolved_keys) | set(SOURCE_TRACK_COLUMNS)
    payload_columns = [
        column
        for column in df.columns
        if column not in payload_excluded
    ]

    working = df.copy()
    if payload_columns:
        working["__payload_signature"] = pd.util.hash_pandas_object(
            working[payload_columns],
            index=False,
            categorize=True,
        ).astype("uint64")
    else:
        working["__payload_signature"] = 0

    grouped = working.groupby(resolved_keys, dropna=False, sort=False)
    sample_limit = 20
    conflict_group_count = 0
    conflict_row_count = 0
    sample_conflicts: list[dict[str, Any]] = []

    for key_values, group_df in grouped:
        source_file_count = int(group_df[SOURCE_TRACK_COLUMNS[0]].nunique())
        payload_signature_count = int(group_df["__payload_signature"].nunique())
        if source_file_count < 2 or payload_signature_count <= 1:
            continue

        conflict_group_count += 1
        conflict_row_count += int(len(group_df))
        if len(sample_conflicts) >= sample_limit:
            continue

        source_counts = (
            group_df[SOURCE_TRACK_COLUMNS[0]]
            .value_counts(dropna=False)
            .sort_index()
        )
        sample_conflicts.append(
            {
                "key": _to_key_dict(key_values, resolved_keys),
                "rowCount": int(len(group_df)),
                "sourceFileCount": source_file_count,
                "payloadSignatureCount": payload_signature_count,
                "sourceRows": [
                    {
                        "sourceFile": str(source_file),
                        "rows": int(rows),
                    }
                    for source_file, rows in source_counts.items()
                ],
            }
        )

    return {
        "enabled": True,
        "policy": "report_only",
        "requestedConflictKeys": list(conflict_keys),
        "resolvedConflictKeys": resolved_keys,
        "hasConflicts": bool(conflict_group_count > 0),
        "conflictGroupCount": int(conflict_group_count),
        "conflictRowCount": int(conflict_row_count),
        "sampleConflicts": sample_conflicts,
        "sampleLimit": sample_limit,
    }


def apply_conflict_policy(
    df: pd.DataFrame,
    conflict_summary: dict[str, Any],
    conflict_policy: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    policy = conflict_policy.strip().lower()
    if policy not in SUPPORTED_CONFLICT_POLICIES:
        raise ValueError(f"不支持的冲突策略: {conflict_policy}")

    summary = dict(conflict_summary)
    summary["policy"] = policy
    summary["droppedByConflictPolicy"] = 0

    if not summary.get("enabled", False):
        return df, summary
    if not summary.get("hasConflicts", False):
        return df, summary

    conflict_group_count = int(summary.get("conflictGroupCount", 0))
    conflict_row_count = int(summary.get("conflictRowCount", 0))

    if policy == "fail":
        raise ValueError(
            "检测到跨文件冲突："
            f"groups={conflict_group_count}, rows={conflict_row_count}。"
            "请调整 conflict keys、启用 last_wins，或先清洗源数据。"
        )

    if policy == "last_wins":
        conflict_keys = list(summary.get("resolvedConflictKeys", []))
        if not conflict_keys:
            raise ValueError("last_wins 需要可解析的 conflict keys。")

        # 按来源顺序稳定排序，保留最后出现记录。
        sorted_df = df.sort_values(
            by=[SOURCE_TRACK_COLUMNS[1], SOURCE_TRACK_COLUMNS[2]],
            kind="stable",
        )
        before_rows = int(len(sorted_df))
        resolved_df = sorted_df.drop_duplicates(
            subset=conflict_keys,
            keep="last",
        ).reset_index(drop=True)
        dropped_rows = before_rows - int(len(resolved_df))
        summary["droppedByConflictPolicy"] = int(max(dropped_rows, 0))
        return resolved_df, summary

    return df, summary


def drop_internal_source_tracking_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in SOURCE_TRACK_COLUMNS:
        if column in result.columns:
            result = result.drop(columns=[column])
    if "__payload_signature" in result.columns:
        result = result.drop(columns=["__payload_signature"])
    return result


def write_conflict_report(
    conflict_report_path: Path,
    source_files: list[Path],
    sheet_name: str,
    summary: dict[str, Any],
) -> None:
    payload = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "sourceExcels": [to_project_relative(path) for path in source_files],
        "sheetName": str(sheet_name),
        "summary": summary,
    }
    conflict_report_path.parent.mkdir(parents=True, exist_ok=True)
    conflict_report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def resolve_columns_case_insensitive(
    columns: list[str],
    requested: list[str],
) -> list[str]:
    column_map = {
        str(column).strip().lower(): str(column)
        for column in columns
    }
    resolved: list[str] = []
    for key in requested:
        target = column_map.get(key.strip().lower())
        if not target:
            raise ValueError(f"字段不存在: {key}")
        if target not in resolved:
            resolved.append(target)
    return resolved


def apply_optional_deduplication(
    df: pd.DataFrame,
    dedupe_keys: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not dedupe_keys:
        return df, {
            "dedupeApplied": False,
            "dedupeKeys": [],
            "droppedDuplicateRows": 0,
        }

    resolved_keys = resolve_columns_case_insensitive(
        columns=[str(column) for column in df.columns],
        requested=dedupe_keys,
    )
    before_rows = int(len(df))
    deduped_df = df.drop_duplicates(subset=resolved_keys, keep="last")
    deduped_df = deduped_df.reset_index(drop=True)
    dropped_rows = before_rows - int(len(deduped_df))

    return deduped_df, {
        "dedupeApplied": True,
        "dedupeKeys": resolved_keys,
        "droppedDuplicateRows": int(dropped_rows),
    }


def evaluate_output_schema(df: pd.DataFrame) -> dict[str, object]:
    columns = [str(column).strip() for column in df.columns]
    col_map = {column.lower(): column for column in columns}

    year_columns = [
        column
        for column in columns
        if YEAR_COL_PATTERN.fullmatch(column)
    ]
    month_columns = [
        column
        for column in columns
        if MONTH_COL_PATTERN.match(column)
    ]

    critical_errors: list[str] = []
    if df.empty:
        critical_errors.append("输出数据为空（0 行）。")
    if not columns:
        critical_errors.append("输出数据无字段。")
    if not year_columns and not month_columns:
        critical_errors.append("未识别到年度或月度时间列。")

    if critical_errors:
        raise ValueError("；".join(critical_errors))

    missing_recommended: dict[str, list[str]] = {}
    for label, candidates in RECOMMENDED_DIMENSION_CANDIDATES.items():
        if any(candidate.lower() in col_map for candidate in candidates):
            continue
        missing_recommended[label] = list(candidates)

    return {
        "yearColumnCount": int(len(year_columns)),
        "monthColumnCount": int(len(month_columns)),
        "hasTimeColumns": bool(year_columns or month_columns),
        "missingRecommendedFields": missing_recommended,
    }


def to_project_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def write_manifest(
    manifest_path: Path,
    source_files: list[Path],
    output_file: Path,
    sheet_name: str,
    df: pd.DataFrame,
    elapsed_seconds: float,
    validation_summary: dict[str, object],
    merge_summary: dict[str, Any],
) -> None:
    source_excel = (
        to_project_relative(source_files[0])
        if source_files
        else ""
    )
    manifest = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "manifestSchemaVersion": MANIFEST_SCHEMA_VERSION,
        "pipelineVersion": "1.0",
        "sourceExcel": source_excel,
        "sourceExcels": [
            to_project_relative(path)
            for path in source_files
        ],
        "inputMode": "multi" if len(source_files) > 1 else "single",
        "sheetName": sheet_name,
        "outputParquet": to_project_relative(output_file),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "columnNames": [str(column) for column in df.columns],
        "outputParquetBytes": int(output_file.stat().st_size),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "validationSummary": validation_summary,
        "mergeSummary": merge_summary,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def convert_jato_to_parquet(
    input_path: str | None,
    input_files: str | None,
    raw_dir: str,
    merge_all_xlsx: bool,
    output_path: str,
    manifest_path: str | None,
    sheet_name: str,
    dedupe_keys: str | None,
    conflict_keys: str | None,
    conflict_policy: str,
    conflict_report_path: str | None,
    job_id: str | None = None,
) -> tuple[Path, Path]:
    logger = get_logger("jato.etl", job_id=job_id)

    def emit(message: str) -> None:
        print(message)
        logger.info(message)

    source_files = resolve_input_files(
        input_path=input_path,
        input_files=input_files,
        raw_dir=raw_dir,
        merge_all_xlsx=merge_all_xlsx,
    )

    output_file = Path(output_path)
    if not output_file.is_absolute():
        output_file = PROJECT_ROOT / output_file
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if manifest_path:
        manifest_file = Path(manifest_path)
        if not manifest_file.is_absolute():
            manifest_file = PROJECT_ROOT / manifest_file
    else:
        manifest_file = output_file.parent / "manifest.json"

    if conflict_report_path:
        resolved_conflict_report_path = Path(conflict_report_path)
        if not resolved_conflict_report_path.is_absolute():
            resolved_conflict_report_path = (
                PROJECT_ROOT / resolved_conflict_report_path
            )
    else:
        resolved_conflict_report_path = output_file.parent / "conflict_report.json"

    if len(source_files) == 1:
        emit(f"🚀 开始转换: {to_project_relative(source_files[0])}")
    else:
        emit(f"🚀 开始批量转换: {len(source_files)} 个 Excel 文件")
    start_time = time.time()

    frames: list[pd.DataFrame] = []
    pre_merge_rows = 0
    for index, source_file in enumerate(source_files, start=1):
        read_start = time.time()
        current_df = read_excel_with_fallback(source_file, sheet_name)
        current_df = add_source_tracking_columns(
            current_df,
            source_file=source_file,
            source_index=index,
        )
        read_elapsed = time.time() - read_start
        pre_merge_rows += int(len(current_df))
        emit(
            "📥 读取 Excel[%s/%s]: %s, 耗时 %.2f 秒, shape=%s"
            % (
                index,
                len(source_files),
                to_project_relative(source_file),
                read_elapsed,
                current_df.shape,
            )
        )
        frames.append(current_df)

    if len(frames) == 1:
        df = frames[0]
    else:
        df = pd.concat(frames, ignore_index=True, sort=False)

    merged_rows = int(len(df))
    emit(f"🧩 合并完成: preMergeRows={pre_merge_rows}, mergedRows={merged_rows}")

    emit("🧹 执行基础清洗与类型标准化...")
    df = normalize_dataframe(df)

    conflict_key_list = parse_csv_list(conflict_keys)
    conflict_summary = detect_cross_file_conflicts(
        df=df,
        conflict_keys=conflict_key_list,
    )
    df, conflict_summary = apply_conflict_policy(
        df=df,
        conflict_summary=conflict_summary,
        conflict_policy=conflict_policy,
    )
    if conflict_summary.get("enabled", False):
        emit(
            "🧭 冲突检测: policy=%s, groups=%s, rows=%s"
            % (
                conflict_summary.get("policy"),
                conflict_summary.get("conflictGroupCount", 0),
                conflict_summary.get("conflictRowCount", 0),
            )
        )
        write_conflict_report(
            conflict_report_path=resolved_conflict_report_path,
            source_files=source_files,
            sheet_name=sheet_name,
            summary=conflict_summary,
        )
        emit(f"🧾 冲突报告: {to_project_relative(resolved_conflict_report_path)}")

    dedupe_result = {
        "dedupeApplied": False,
        "dedupeKeys": [],
        "droppedDuplicateRows": 0,
    }
    dedupe_key_list = parse_csv_list(dedupe_keys)
    if dedupe_key_list:
        emit(f"🔁 执行去重: keys={dedupe_key_list}")
        df, dedupe_result = apply_optional_deduplication(df, dedupe_key_list)
        emit(
            "✅ 去重完成: dropped=%s, rowsAfterDedupe=%s"
            % (
                dedupe_result.get("droppedDuplicateRows", 0),
                len(df),
            )
        )

    df = drop_internal_source_tracking_columns(df)

    emit("🔍 执行输出字段校验...")
    schema_summary = evaluate_output_schema(df)
    missing_recommended = schema_summary.get("missingRecommendedFields", {})
    if isinstance(missing_recommended, dict) and missing_recommended:
        missing_labels = ", ".join(sorted(missing_recommended.keys()))
        emit(f"⚠️ 推荐字段缺失: {missing_labels}")

    emit(f"💾 正在写入 Parquet: {to_project_relative(output_file)}")
    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    elapsed = time.time() - start_time
    write_manifest(
        manifest_path=manifest_file,
        source_files=source_files,
        output_file=output_file,
        sheet_name=sheet_name,
        df=df,
        elapsed_seconds=elapsed,
        validation_summary=schema_summary,
        merge_summary={
            "sourceFileCount": int(len(source_files)),
            "preMergeRows": int(pre_merge_rows),
            "mergedRows": int(merged_rows),
            "finalRows": int(len(df)),
            "conflictSummary": conflict_summary,
            **dedupe_result,
        },
    )

    emit(f"✅ 转换完成: {to_project_relative(output_file)}")
    emit(f"🧾 Manifest: {to_project_relative(manifest_file)}")
    emit(f"⏱️ 总耗时: {elapsed:.2f} 秒")
    return output_file, manifest_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将 JATO Excel 转换为 Parquet，并生成 manifest。"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Excel 输入文件路径（可选）。未提供时会从 01_RAW_DATA 自动选择。",
    )
    parser.add_argument(
        "--input-files",
        type=str,
        default=None,
        help="多个 Excel 输入路径（逗号分隔）。",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=str(DEFAULT_RAW_DIR),
        help="RawData 目录（当未传 --input 时生效）。",
    )
    parser.add_argument(
        "--merge-all-xlsx",
        action="store_true",
        help="合并 raw-dir 下全部 xlsx 文件（按时间顺序）。",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_FILE),
        help="Parquet 输出文件路径。",
    )
    parser.add_argument(
        "--manifest",
        type=str,
        default=None,
        help="Manifest 输出路径（默认与 Parquet 同目录下 manifest.json）。",
    )
    parser.add_argument(
        "--sheet",
        type=str,
        default=DEFAULT_SHEET,
        help="Excel sheet 名称。",
    )
    parser.add_argument(
        "--dedupe-keys",
        type=str,
        default=None,
        help="可选去重字段（逗号分隔，大小写不敏感，保留后出现记录）。",
    )
    parser.add_argument(
        "--conflict-keys",
        type=str,
        default=None,
        help="冲突检测业务键（逗号分隔）。",
    )
    parser.add_argument(
        "--conflict-policy",
        type=str,
        choices=sorted(SUPPORTED_CONFLICT_POLICIES),
        default="report_only",
        help="冲突处理策略：report_only/fail/last_wins。",
    )
    parser.add_argument(
        "--conflict-report",
        type=str,
        default=None,
        help="冲突报告输出路径（默认输出目录下 conflict_report.json）。",
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
    job_id = args.job_id or build_job_id("etl")
    logger = get_logger("jato.etl", job_id=job_id)
    try:
        convert_jato_to_parquet(
            input_path=args.input,
            input_files=args.input_files,
            raw_dir=args.raw_dir,
            merge_all_xlsx=bool(args.merge_all_xlsx),
            output_path=args.output,
            manifest_path=args.manifest,
            sheet_name=args.sheet,
            dedupe_keys=args.dedupe_keys,
            conflict_keys=args.conflict_keys,
            conflict_policy=args.conflict_policy,
            conflict_report_path=args.conflict_report,
            job_id=job_id,
        )
    except Exception as error:
        logger.error("ETL失败[%s] %s", type(error).__name__, error)
        print(
            f"❌ ETL失败[{type(error).__name__}] {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
