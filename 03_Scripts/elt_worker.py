import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from logging_utils import build_job_id, get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = PROJECT_ROOT / "01_RAW_DATA"
DEFAULT_OUTPUT_FILE = (
    PROJECT_ROOT / "04_Processed_data/jato_full_archive.parquet"
)
DEFAULT_SHEET = "Data Export"
MANIFEST_SCHEMA_VERSION = "1.1"

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


def find_latest_xlsx(raw_dir: Path) -> Path:
    candidates = [
        path
        for path in raw_dir.glob("*.xlsx")
        if not path.name.startswith("~$")
    ]
    if not candidates:
        raise FileNotFoundError(f"未在目录中找到 Excel 文件: {raw_dir}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def resolve_input_file(input_path: str | None, raw_dir: str) -> Path:
    if input_path:
        candidate = Path(input_path)
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        if not candidate.exists():
            raise FileNotFoundError(f"输入文件不存在: {candidate}")
        return candidate

    raw_dir_path = Path(raw_dir)
    if not raw_dir_path.is_absolute():
        raw_dir_path = PROJECT_ROOT / raw_dir_path
    if not raw_dir_path.exists():
        raise FileNotFoundError(f"RawData 目录不存在: {raw_dir_path}")

    preferred = raw_dir_path / "JATO-2026.1.xlsx"
    if preferred.exists():
        return preferred

    return find_latest_xlsx(raw_dir_path)


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


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip() for column in result.columns]

    object_cols = result.select_dtypes(include=["object"]).columns
    for column in object_cols:
        result[column] = result[column].astype("string")

    return result


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
    source_file: Path,
    output_file: Path,
    sheet_name: str,
    df: pd.DataFrame,
    elapsed_seconds: float,
    validation_summary: dict[str, object],
) -> None:
    manifest = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "manifestSchemaVersion": MANIFEST_SCHEMA_VERSION,
        "pipelineVersion": "1.0",
        "sourceExcel": to_project_relative(source_file),
        "sheetName": sheet_name,
        "outputParquet": to_project_relative(output_file),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "columnNames": [str(column) for column in df.columns],
        "outputParquetBytes": int(output_file.stat().st_size),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "validationSummary": validation_summary,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def convert_jato_to_parquet(
    input_path: str | None,
    raw_dir: str,
    output_path: str,
    manifest_path: str | None,
    sheet_name: str,
    job_id: str | None = None,
) -> tuple[Path, Path]:
    logger = get_logger("jato.etl", job_id=job_id)

    def emit(message: str) -> None:
        print(message)
        logger.info(message)

    input_file = resolve_input_file(input_path, raw_dir)

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

    emit(f"🚀 开始转换: {to_project_relative(input_file)}")
    start_time = time.time()

    df = read_excel_with_fallback(input_file, sheet_name)
    t1 = time.time()
    emit(f"📥 读取 Excel 耗时: {t1 - start_time:.2f} 秒, shape={df.shape}")

    emit("🧹 执行基础清洗与类型标准化...")
    df = normalize_dataframe(df)

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
        source_file=input_file,
        output_file=output_file,
        sheet_name=sheet_name,
        df=df,
        elapsed_seconds=elapsed,
        validation_summary=schema_summary,
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
        "--raw-dir",
        type=str,
        default=str(DEFAULT_RAW_DIR),
        help="RawData 目录（当未传 --input 时生效）。",
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
            raw_dir=args.raw_dir,
            output_path=args.output,
            manifest_path=args.manifest,
            sheet_name=args.sheet,
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
