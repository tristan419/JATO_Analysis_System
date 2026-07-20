from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import shlex
import shutil
import subprocess
import sys
import threading
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote
from uuid import uuid4

import pandas as pd
from fastapi import HTTPException, UploadFile

from app.core.config import (
    JATO_MONTHLY_UPDATE_JOB_ROOT,
    JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES,
    PROJECT_ROOT,
)
from app.services.hermes_pipeline_status_service import write_pipeline_status

MONTHLY_UPDATE_JOB_ROOT = JATO_MONTHLY_UPDATE_JOB_ROOT
RAW_DATA_ROOT = PROJECT_ROOT / "01_RAW_DATA"
BASELINE_ROOT = RAW_DATA_ROOT / "baseline"
PATCHES_ROOT = RAW_DATA_ROOT / "patches"
HISTORY_ARCHIVE_ROOT = RAW_DATA_ROOT / "historyDataArchive"
PREPARE_SCRIPT_PATH = (
    PROJECT_ROOT / "03_Scripts" / "data_pipeline" / "prepare_monthly_raw_update.py"
)
REBUILD_SCRIPT_PATH = (
    PROJECT_ROOT / "03_Scripts" / "data_pipeline" / "rebuild_from_parquet.py"
)
SINGLE_COUNTRY_ETL_SCRIPT_PATH = PROJECT_ROOT / "03_Scripts" / "elt_worker.py"
STATE_FILENAME = "job_state.json"
LOG_FILENAME = "job.log"
UPLOAD_STATE_FILENAME = "upload_state.json"
MONTH_PATTERN = re.compile(r"(20\d{2})[-./]?(0?[1-9]|1[0-2])")
CODE_BLOCK_PATTERN = re.compile(r"```bash\s*(.*?)\s*```", re.DOTALL)
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
UPLOAD_CHUNK_SIZE_BYTES = JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES
DEFAULT_UPLOAD_SHEET_NAME = "Data Export"
MONTH_COLUMN_PATTERN = re.compile(
    r"^\d{4}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)
YEAR_COLUMN_PATTERN = re.compile(r"^\d{4}$")
YTD_COLUMN_PATTERN = re.compile(r"^YTD\s+\d{4}\s+\([A-Za-z]{3}\)$", re.IGNORECASE)
BATCH_ID_PATTERN = re.compile(r"^(20\d{2}-\d{2})-r(\d+)$")
COUNTRY_COLUMN_CANDIDATES = ("国家", "country")
SAFE_CLEANUP_TIER = "safe"
CAUTIOUS_CLEANUP_TIER = "cautious"
PROTECTED_CLEANUP_TIER = "protected"
ALLOWED_CLEANUP_TIERS = {SAFE_CLEANUP_TIER, CAUTIOUS_CLEANUP_TIER}
SALES_DOUBLING_RATIO_MIN = 1.98
SALES_DOUBLING_RATIO_MAX = 2.02
SALES_DOUBLING_MIN_REFERENCE_SALES = 1000.0
SALES_DOUBLING_MIN_ABSOLUTE_DELTA = 1000.0
SALES_DOUBLING_MIN_MONTH_COUNT = 2
SALES_DOUBLING_SAMPLE_LIMIT = 6
DEPRECATED_OPTIONAL_STATIC_COLUMNS = frozenset(
    {
        "Base price",
        "CO2 level - (g/km) combined",
        "MSRP including delivery charge",
        "Maximum power kW",
        "WLTP Emission combined",
        "cargo volume (l)",
        "curb weight (kg)",
    }
)
STATIC_CARRY_FORWARD_KEY_CANDIDATES = (
    "国家",
    "Countries",
    "Registration type",
    "Make group",
    "Make",
    "Model group",
    "Model",
    "Version name",
    "Powertrain type",
    "Trim level",
    "Body type",
    "Fuel type",
    "Transmission type",
    "Driven wheels",
    "Battery type",
    "Battery kwh",
    "Useable battery kilowatt hour (kWh)",
    "Battery range",
    "Seating capacity",
    "length (mm)",
    "width (mm)",
    "height (mm)",
    "wheelbase (mm)",
)
_WRITE_LOCK = threading.Lock()
_RUNNING_THREADS: dict[str, threading.Thread] = {}
RUNNING_JOB_STATUSES = {"queued", "running"}
PROCESS_TERMINATE_GRACE_SECONDS = 8
RUNNING_LOG_STALE_SECONDS = 15 * 60


class _JobCancelled(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _relative_to_project(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _project_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    candidate = Path(value)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _job_dir(job_id: str) -> Path:
    return MONTHLY_UPDATE_JOB_ROOT / job_id


def _job_state_path(job_id: str) -> Path:
    return _job_dir(job_id) / STATE_FILENAME


def _job_log_path(job_id: str) -> Path:
    return _job_dir(job_id) / LOG_FILENAME


def _processed_data_root() -> Path:
    return PROJECT_ROOT / "04_Processed_data"


def _active_data_paths() -> dict[str, Path]:
    processed_root = _processed_data_root()
    return {
        "parquet": processed_root / "jato_full_archive.parquet",
        "manifest": processed_root / "manifest.json",
        "partition": processed_root / "partitioned_dataset_v1",
        "fingerprint": processed_root / "dataset_fingerprint.json",
        "refreshReport": processed_root / "refresh_job_report.json",
        "backupRoot": processed_root / ".refresh_backups",
    }


def _time_sort_key(label: str) -> tuple[int, int, str]:
    text = str(label).strip()
    if MONTH_COLUMN_PATTERN.match(text):
        parsed = datetime.strptime(text.title(), "%Y %b")
        return (parsed.year, parsed.month, text)
    if YEAR_COLUMN_PATTERN.fullmatch(text):
        return (int(text), 0, text)
    return (9999, 12, text)


def _series_has_data(series: pd.Series) -> bool:
    if series.empty:
        return False
    if (
        pd.api.types.is_string_dtype(series.dtype)
        or pd.api.types.is_object_dtype(series.dtype)
    ):
        normalized = series.astype("string").fillna("").str.strip()
        return bool((normalized != "").any())
    return bool(series.notna().any())


def _series_missing_value_mask(series: pd.Series) -> pd.Series:
    missing = series.isna()
    if (
        pd.api.types.is_string_dtype(series.dtype)
        or pd.api.types.is_object_dtype(series.dtype)
    ):
        missing = missing | series.astype("string").fillna("").str.strip().eq("")
    return missing


def _find_country_column(columns: list[str]) -> str | None:
    lookup = {str(column).strip().lower(): str(column) for column in columns}
    for candidate in COUNTRY_COLUMN_CANDIDATES:
        column = lookup.get(candidate.lower())
        if column:
            return column
    return None


def _detect_month_columns(columns: list[str]) -> list[str]:
    return sorted(
        [
            str(column).strip()
            for column in columns
            if MONTH_COLUMN_PATTERN.match(str(column).strip())
        ],
        key=_time_sort_key,
    )


def _collect_dataset_country_latest_months(path: Path) -> dict[str, str | None]:
    frame = pd.read_parquet(path)
    frame.columns = [str(column).strip() for column in frame.columns]
    country_col = _find_country_column(list(frame.columns))
    if country_col is None:
        raise HTTPException(
            status_code=409,
            detail=f"无法从 {path.name} 识别国家列，不能执行 publish 校验。",
        )
    month_columns = _detect_month_columns(list(frame.columns))
    if not month_columns:
        raise HTTPException(
            status_code=409,
            detail=f"无法从 {path.name} 识别月份列，不能执行 publish 校验。",
        )

    info: dict[str, str | None] = {}
    grouped = frame.groupby(country_col, dropna=False, sort=False)
    for raw_country, group_df in grouped:
        country = str(raw_country).strip()
        if not country:
            continue
        present_months = [
            column
            for column in month_columns
            if column in group_df.columns and _series_has_data(group_df[column])
        ]
        info[country] = present_months[-1] if present_months else None
    return info


def _candidate_fingerprint_id(artifacts: dict[str, Any]) -> str:
    """Bind a human approval to the exact candidate that was reviewed."""
    candidate_path = _project_path(str(artifacts.get("stagingOutputPath") or "").strip())
    if candidate_path is None or not candidate_path.exists():
        raise HTTPException(status_code=409, detail="缺少 candidate parquet，不能确认 Review。")
    manifest_value = str(artifacts.get("manifestPath") or "").strip()
    manifest_path = _project_path(manifest_value) if manifest_value else None
    hasher = hashlib.sha256()
    hasher.update(_sha256_hex_for_path(candidate_path).encode("ascii"))
    if manifest_path is not None and manifest_path.exists():
        hasher.update(_sha256_hex_for_path(manifest_path).encode("ascii"))
    return hasher.hexdigest()


def _load_parquet_country_subset(path: Path, country: str, *, path_label: str) -> pd.DataFrame:
    """Read exactly one country from a parquet file; never materialize the archive."""
    try:
        import pyarrow.parquet as pq

        schema = pq.read_schema(path)
        country_column = _find_country_column([str(column) for column in schema.names])
        if country_column is None:
            raise HTTPException(status_code=409, detail=f"{path_label} 缺少国家列。")
        frame = pd.read_parquet(path, filters=[(country_column, "==", country)])
        frame.columns = [str(column).strip() for column in frame.columns]
        return frame
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=409,
            detail=f"读取 {path_label} 的 {country} 分区失败。",
        ) from exc


def _load_active_country_partition_subset(partition_root: Path, country: str) -> pd.DataFrame:
    country_dir = partition_root / f"国家={quote(country, safe='')}"
    if not country_dir.exists():
        raise HTTPException(status_code=409, detail=f"找不到 active 的 {country} 国家分区。")
    try:
        frame = pd.read_parquet(country_dir)
        frame.columns = [str(column).strip() for column in frame.columns]
        if _find_country_column(list(frame.columns)) is None:
            frame["国家"] = country
        return frame
    except Exception as exc:
        raise HTTPException(
            status_code=409,
            detail=f"读取 active 的 {country} 国家分区失败。",
        ) from exc


def _untouched_partition_snapshot(
    *,
    partition_root: Path,
    country: str | None = None,
    countries: list[str] | None = None,
) -> dict[str, Any]:
    """Fingerprint non-target manifest entries without touching their parquet files."""
    manifest_path = partition_root / "manifest.json"
    manifest = _read_json_if_exists(_relative_to_project(manifest_path)) or {}
    partition_stats = manifest.get("partitionStats")
    partition_columns = manifest.get("partitionColumns")
    if not isinstance(partition_stats, dict):
        return {"status": "unavailable", "reason": "missing_partition_stats"}
    column = (
        str(partition_columns[0])
        if isinstance(partition_columns, list) and partition_columns
        else "国家"
    )
    target_countries = _ordered_distinct_strings(
        [*(countries or []), *([country] if country else [])]
    )
    target_prefixes = [
        f"{column}={quote(target_country, safe='')}"
        for target_country in target_countries
    ]
    untouched = {
        str(key): value
        for key, value in partition_stats.items()
        if not any(str(key).startswith(prefix) for prefix in target_prefixes)
    }
    encoded = json.dumps(untouched, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return {
        "status": "pass",
        "targetPartitionPrefix": target_prefixes[0] if len(target_prefixes) == 1 else None,
        "targetPartitionPrefixes": target_prefixes,
        "untouchedPartitionCount": len(untouched),
        "untouchedPartitionFingerprint": hashlib.sha256(encoded).hexdigest(),
    }


def _verify_untouched_partition_stability(*, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    if before.get("status") != "pass" or after.get("status") != "pass":
        return {"status": "unavailable", "before": before, "after": after}
    changed = before.get("untouchedPartitionFingerprint") != after.get("untouchedPartitionFingerprint")
    return {
        "status": "fail" if changed else "pass",
        "untouchedPartitionCount": int(before.get("untouchedPartitionCount", 0) or 0),
        "beforeFingerprint": before.get("untouchedPartitionFingerprint"),
        "afterFingerprint": after.get("untouchedPartitionFingerprint"),
    }


def _latest_month_from_frame(frame: pd.DataFrame) -> str | None:
    for column in reversed(_detect_month_columns(list(frame.columns))):
        if _series_has_data(frame[column]):
            return column
    return None


def _is_derived_ytd_column(column: str) -> bool:
    return bool(YTD_COLUMN_PATTERN.fullmatch(str(column).strip()))


def _single_country_schema_contract(*, active_frame: pd.DataFrame, candidate_frame: pd.DataFrame) -> dict[str, list[str]]:
    active_months = set(_detect_month_columns(list(active_frame.columns)))
    candidate_months = set(_detect_month_columns(list(candidate_frame.columns)))
    active_static = set(active_frame.columns) - active_months
    candidate_static = set(candidate_frame.columns) - candidate_months
    missing = sorted(active_static - candidate_static)
    null_only: list[str] = []
    derived_ytd: list[str] = []
    deprecated_optional: list[str] = []
    material: list[str] = []
    for column in missing:
        if not _series_has_data(active_frame[column]):
            null_only.append(column)
        elif _is_derived_ytd_column(column):
            derived_ytd.append(column)
        elif column in DEPRECATED_OPTIONAL_STATIC_COLUMNS:
            deprecated_optional.append(column)
        else:
            material.append(column)
    return {
        "missing": missing,
        "missingNullOnly": null_only,
        "missingDerivedYtd": derived_ytd,
        "missingDeprecatedOptional": deprecated_optional,
        "missingMaterial": material,
        "extra": sorted(candidate_static - active_static),
    }


def _single_country_configuration_key_columns(frame: pd.DataFrame) -> list[str]:
    month_columns = set(_detect_month_columns(list(frame.columns)))
    return [
        str(column)
        for column in frame.columns
        if str(column) not in month_columns
        and not YEAR_COLUMN_PATTERN.fullmatch(str(column).strip())
        and not _is_derived_ytd_column(str(column))
    ]


def _single_country_historical_sales_stability(
    *,
    active_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
    active_latest_month: str | None,
) -> dict[str, Any]:
    if active_latest_month is None:
        return {"status": "unavailable", "reason": "active_latest_month_missing"}
    active_months = set(_detect_month_columns(list(active_frame.columns)))
    candidate_months = set(_detect_month_columns(list(candidate_frame.columns)))
    historical_months = sorted(
        [month for month in active_months if _time_sort_key(month) <= _time_sort_key(active_latest_month)],
        key=_time_sort_key,
    )
    missing = [month for month in historical_months if month not in candidate_months]
    if missing:
        return {"status": "fail", "reason": "candidate_missing_historical_months", "missingMonths": missing}

    active_sales = active_frame[historical_months].apply(pd.to_numeric, errors="coerce").fillna(0)
    candidate_sales = candidate_frame[historical_months].apply(pd.to_numeric, errors="coerce").fillna(0)
    country_samples: list[dict[str, Any]] = []
    make_samples: list[dict[str, Any]] = []

    def compare(
        left: pd.Series,
        right: pd.Series,
        *,
        scope: str,
        destination: list[dict[str, Any]],
    ) -> None:
        for month in historical_months:
            left_value = float(left.get(month, 0) or 0)
            right_value = float(right.get(month, 0) or 0)
            if left_value != right_value:
                destination.append({
                    "scope": scope,
                    "month": month,
                    "activeSales": _serialize_numeric_value(left_value),
                    "candidateSales": _serialize_numeric_value(right_value),
                    "deltaSales": _serialize_numeric_value(right_value - left_value),
                })

    compare(
        active_sales.sum(),
        candidate_sales.sum(),
        scope="country",
        destination=country_samples,
    )
    compared_make_count = 0
    if "Make" in active_frame.columns and "Make" in candidate_frame.columns:
        active_grouped = active_sales.groupby(active_frame["Make"].astype("string").fillna("").str.strip(), dropna=False).sum()
        candidate_grouped = candidate_sales.groupby(candidate_frame["Make"].astype("string").fillna("").str.strip(), dropna=False).sum()
        for make in sorted(set(active_grouped.index) | set(candidate_grouped.index)):
            compared_make_count += 1
            compare(
                active_grouped.loc[make] if make in active_grouped.index else pd.Series(0, index=historical_months),
                candidate_grouped.loc[make] if make in candidate_grouped.index else pd.Series(0, index=historical_months),
                scope=f"make:{make}",
                destination=make_samples,
            )
    samples = [*country_samples, *make_samples]
    cause = None
    if country_samples:
        cause = "historical_sales_changed"
    elif make_samples:
        cause = "make_dimension_reclassification"
    return {
        "status": "pass" if not samples else "fail",
        "reason": cause,
        "comparedThrough": active_latest_month,
        "comparedMonthCount": len(historical_months),
        "comparedMakeCount": compared_make_count,
        "mismatchCount": len(samples),
        "countryMismatchCount": len(country_samples),
        "makeMismatchCount": len(make_samples),
        "impactedMakes": sorted(
            {
                str(sample["scope"]).removeprefix("make:")
                for sample in make_samples
            }
        ),
        "impactedMonths": sorted(
            {str(sample["month"]) for sample in samples},
            key=_time_sort_key,
        ),
        "mismatchSamples": samples[:20],
    }


def _single_country_source_feedback(*, rule_id: str, country: str, metrics: dict[str, Any]) -> str | None:
    """Translate measured validation failures into copy-ready washer feedback."""
    def columns(name: str) -> str:
        values = metrics.get(name)
        return "、".join(str(value) for value in values) if isinstance(values, list) else ""

    if rule_id == "SC001":
        return f"请重新导出 {country} 的完整月份列。当前文件未识别到有效月份；请保留历史月份和本次最新月份，并确保销量字段为可解析数字。"
    if rule_id == "SC002":
        return f"请检查 {country} 的月份销量值。文件包含月份标题但没有可用数值；请不要把销量列清空、转成文本或用格式符号替代数值。"
    if rule_id == "SC003":
        return f"请提供不早于当前 active {metrics.get('active') or '月份'} 的 {country} 数据。本次最新月份为 {metrics.get('candidate') or '未知'}，发生回退；请确认导出筛选条件包含本次要推进的最新月份。"
    if rule_id == "SC004":
        return f"请去除 {country} 文件中的完全相同配置行（检测到 {metrics.get('duplicateRows', 0)} 行）。只删除所有配置字段均相同的重复记录；价格、Registration type、动力或车身不同的版本不能合并。"
    if rule_id == "SC005":
        return f"请修正 {country} 文件中的负销量（检测到 {metrics.get('negativeSalesCells', 0)} 个单元格）。请按原始 JATO 导出确认更正、冲销或缺失值的处理方式，不能直接把负数绝对值化。"
    if rule_id == "SC006":
        return f"请检查 {country} 的历史月份是否被重复拼接或重复累计{f'：{columns('months')}' if columns('months') else ''}。候选销量接近 active 的两倍；请从原始导出重新生成，不要把历史全量文件再追加到已有历史上。"
    if rule_id == "SC007":
        return f"请仅提供 {country} 的单国数据。系统发现未上传国家的分区也发生变化；请检查洗数流程是否混入其他国家或复写了共享数据。"
    if rule_id == "SC008":
        return "本次无法证明未上传国家未被影响。请保留单国文件的国家范围、源文件哈希和分区清单，以便重新提交时完成隔离校验。"
    if rule_id == "SC009":
        return f"请在 {country} 的洗数后 Data Export 中保留业务字段：{columns('missingMaterialColumns') or '见 Review 明细'}。这些列在当前 active 数据中有实际值，不能用 Retail price 或其他相近字段自动替代；请从原始 JATO 导出补齐后重新输出同一月份文件。"
    if rule_id == "SC010":
        return f"{country} 文件新增字段：{columns('extraColumns') or '见 Review 明细'}。请提供字段定义、单位和是否应保留的确认；新增列不应覆盖或改名现有业务列。"
    if rule_id == "SC011":
        if metrics.get("reason") == "make_dimension_reclassification":
            return f"{country} 的国家历史月销量总量与 active 一致，但 Make 归类发生变化。受影响品牌：{columns('impactedMakes') or '见 Review 明细'}；受影响月份：{columns('impactedMonths') or '见 Review 明细'}。请确认是否调整了品牌/车型映射；若为有意重分类，请提供旧 Make → 新 Make/Model 映射和业务确认，否则恢复已发布历史归类。"
        return f"请恢复 {country} 在 {metrics.get('comparedThrough') or '已有'} 之前的历史销量。系统发现 {metrics.get('mismatchCount', 0)} 处历史销量差异；本次更新只能新增或修正经确认的最新月份，不能重写已发布历史月份。"
    if rule_id == "SC012":
        row_delta = int(metrics.get("rowDelta", 0) or 0)
        stability = metrics.get("historicalSalesStability")
        history_note = "历史销量已通过核对" if isinstance(stability, dict) and stability.get("status") == "pass" else "历史销量需要一并复核"
        return f"{country} 本次配置行较 active {'减少' if row_delta < 0 else '增加'} {abs(row_delta)} 行，{history_note}。请说明洗数时的去重、零销量配置过滤和车型下架规则，并提供清洗前后配置行数；不要仅为凑行数复制或补造车型。"
    if rule_id == "SC013":
        return f"{country} 缺少旧月份 YTD 派生列：{columns('missingDerivedYtdColumns') or '见 Review 明细'}。这不是发布 blocker；请确认最新月份的 YTD 字段仍由 Jan 至当月销量计算，并在后续导出中保持 YTD 列命名和口径一致。"
    if rule_id == "SC014":
        return f"{country} 缺少已停用的可选静态字段：{columns('missingDeprecatedOptionalColumns') or '见 Review 明细'}。无需为本次月更补齐；系统只会在配置键匹配且 active 旧值一致时沿用旧值，新配置或旧值冲突的配置保持为空，且不会用 Retail price 等相近字段冒充原字段。"
    return None


def _ordered_distinct_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw_value in values:
        value = str(raw_value).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _serialize_numeric_value(value: Any) -> int | float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    if number.is_integer():
        return int(number)
    return number


def _load_monthly_sales_frame(path: Path, *, path_label: str) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(status_code=409, detail=f"{path_label} 不存在：{path}")
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return _load_dataset_frame(path)
    if suffix in ALLOWED_UPLOAD_EXTENSIONS:
        frame = _read_excel_with_fallback(path, sheet_name=0)
        frame.columns = [str(column).strip() for column in frame.columns]
        return frame
    raise HTTPException(
        status_code=409,
        detail=f"{path_label} 不是支持的 tabular 数据文件：{path.name}",
    )


def _collect_country_monthly_sales(
    frame: pd.DataFrame,
    *,
    countries: list[str],
    path_label: str,
) -> dict[str, dict[str, int | float]]:
    if frame.empty or not countries:
        return {}
    country_column = _find_country_column(list(frame.columns))
    if country_column is None:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 缺少国家列，无法生成逐月销量核对表。",
        )
    month_columns = _detect_month_columns(list(frame.columns))
    if not month_columns:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 缺少月份列，无法生成逐月销量核对表。",
        )
    normalized_countries = frame[country_column].astype("string").fillna("").str.strip()
    working = frame.loc[
        normalized_countries.isin(countries), [country_column, *month_columns]
    ].copy()
    if working.empty:
        return {}
    working[country_column] = normalized_countries.loc[working.index]
    for column in month_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    grouped = working.groupby(country_column, dropna=False, sort=False)[
        month_columns
    ].sum(min_count=1)
    result: dict[str, dict[str, int | float]] = {}
    for country in countries:
        if country not in grouped.index:
            continue
        values = grouped.loc[country]
        result[country] = {
            month: serialized
            for month in month_columns
            if (serialized := _serialize_numeric_value(values.get(month))) is not None
        }
    return result


def _collect_frame_countries(frame: pd.DataFrame, *, path_label: str) -> list[str]:
    country_column = _find_country_column(list(frame.columns))
    if country_column is None:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 缺少国家列，无法执行 publish 销量防重校验。",
        )
    values = frame[country_column].astype("string").fillna("").str.strip()
    return _ordered_distinct_strings(values.tolist())


def _is_near_sales_doubling(
    *,
    reference_sales: int | float | None,
    candidate_sales: int | float | None,
) -> tuple[bool, float | None]:
    if reference_sales is None or candidate_sales is None:
        return False, None
    reference = float(reference_sales)
    candidate = float(candidate_sales)
    if reference < SALES_DOUBLING_MIN_REFERENCE_SALES:
        return False, None
    if abs(candidate - reference) < SALES_DOUBLING_MIN_ABSOLUTE_DELTA:
        return False, None
    ratio = candidate / reference if reference else 0.0
    if SALES_DOUBLING_RATIO_MIN <= ratio <= SALES_DOUBLING_RATIO_MAX:
        return True, ratio
    return False, ratio


def _find_publish_sales_doubling_anomalies(
    *,
    active_parquet_path: Path,
    candidate_parquet_path: Path,
) -> list[dict[str, Any]]:
    """Detect likely duplicate country merges before candidate data becomes active."""
    active_frame = _load_monthly_sales_frame(
        active_parquet_path,
        path_label="当前 active 数据集",
    )
    candidate_frame = _load_monthly_sales_frame(
        candidate_parquet_path,
        path_label="candidate 数据集",
    )
    active_countries = set(
        _collect_frame_countries(active_frame, path_label="当前 active 数据集")
    )
    candidate_countries = set(
        _collect_frame_countries(candidate_frame, path_label="candidate 数据集")
    )
    countries = sorted(active_countries & candidate_countries)
    if not countries:
        return []

    active_totals = _collect_country_monthly_sales(
        active_frame,
        countries=countries,
        path_label="当前 active 数据集",
    )
    candidate_totals = _collect_country_monthly_sales(
        candidate_frame,
        countries=countries,
        path_label="candidate 数据集",
    )

    anomalies: list[dict[str, Any]] = []
    for country in countries:
        reference_months = active_totals.get(country, {})
        candidate_months = candidate_totals.get(country, {})
        common_months = sorted(
            set(reference_months.keys()) & set(candidate_months.keys()),
            key=_time_sort_key,
        )
        suspicious_months: list[dict[str, Any]] = []
        for month in common_months:
            reference_sales = reference_months.get(month)
            candidate_sales = candidate_months.get(month)
            is_doubled, ratio = _is_near_sales_doubling(
                reference_sales=reference_sales,
                candidate_sales=candidate_sales,
            )
            if not is_doubled:
                continue
            suspicious_months.append(
                {
                    "month": month,
                    "referenceSales": reference_sales,
                    "candidateSales": candidate_sales,
                    "ratio": round(float(ratio or 0), 3),
                }
            )

        if len(suspicious_months) < SALES_DOUBLING_MIN_MONTH_COUNT:
            continue

        recent_months = common_months[-12:]
        reference_rolling = sum(
            float(reference_months.get(month) or 0) for month in recent_months
        )
        candidate_rolling = sum(
            float(candidate_months.get(month) or 0) for month in recent_months
        )
        rolling_ratio = (
            candidate_rolling / reference_rolling if reference_rolling else None
        )
        anomalies.append(
            {
                "country": country,
                "suspiciousMonthCount": len(suspicious_months),
                "sampleMonths": suspicious_months[:SALES_DOUBLING_SAMPLE_LIMIT],
                "referenceRolling12": _serialize_numeric_value(reference_rolling),
                "candidateRolling12": _serialize_numeric_value(candidate_rolling),
                "rolling12Ratio": (
                    round(float(rolling_ratio), 3)
                    if rolling_ratio is not None
                    else None
                ),
            }
        )
    return anomalies


def _render_sales_doubling_anomalies(anomalies: list[dict[str, Any]]) -> str:
    rendered_items: list[str] = []
    for item in anomalies[:5]:
        country = str(item.get("country", ""))
        sample_months = item.get("sampleMonths")
        months = []
        if isinstance(sample_months, list):
            for month_item in sample_months[:3]:
                if isinstance(month_item, dict):
                    months.append(str(month_item.get("month", "")))
        month_text = ",".join(month for month in months if month) or "-"
        ratio = item.get("rolling12Ratio")
        ratio_text = f", rolling12={ratio}x" if ratio is not None else ""
        rendered_items.append(f"{country}({month_text}{ratio_text})")
    extra = " 等" if len(anomalies) > 5 else ""
    return "；".join(rendered_items) + extra


def _resolve_review_reference_dataset(
    artifacts: dict[str, Any]
) -> tuple[Path | None, str]:
    active_parquet_path = _active_data_paths()["parquet"]
    if active_parquet_path.exists():
        return active_parquet_path, "网站当前 active"
    baseline_path = _project_path(str(artifacts.get("baselinePath") or "").strip())
    if baseline_path and baseline_path.exists():
        return baseline_path, "baseline"
    return None, "-"


def _build_country_monthly_sales_summary(
    *,
    countries: list[str],
    candidate_path: Path,
    reference_path: Path | None,
) -> list[dict[str, Any]]:
    candidate_frame = _load_monthly_sales_frame(
        candidate_path, path_label="candidate 数据集"
    )
    candidate_totals = _collect_country_monthly_sales(
        candidate_frame,
        countries=countries,
        path_label="candidate 数据集",
    )
    reference_totals: dict[str, dict[str, int | float]] = {}
    if reference_path is not None:
        reference_frame = _load_monthly_sales_frame(
            reference_path, path_label="参考数据集"
        )
        reference_totals = _collect_country_monthly_sales(
            reference_frame,
            countries=countries,
            path_label="参考数据集",
        )

    summaries: list[dict[str, Any]] = []
    for country in countries:
        reference_months = reference_totals.get(country, {})
        candidate_months = candidate_totals.get(country, {})
        months = sorted(
            set(reference_months.keys()) | set(candidate_months.keys()),
            key=_time_sort_key,
        )
        if not months:
            continue
        rows: list[dict[str, Any]] = []
        for month in months:
            reference_sales = reference_months.get(month)
            candidate_sales = candidate_months.get(month)
            if reference_sales is not None and candidate_sales is not None:
                delta_sales = _serialize_numeric_value(candidate_sales - reference_sales)
                change_status = "unchanged" if delta_sales == 0 else "changed"
            elif candidate_sales is not None:
                delta_sales = None
                change_status = "added"
            else:
                delta_sales = None
                change_status = "removed"
            rows.append(
                {
                    "month": month,
                    "referenceSales": reference_sales,
                    "candidateSales": candidate_sales,
                    "deltaSales": delta_sales,
                    "changeStatus": change_status,
                }
            )
        summaries.append({"country": country, "rows": rows})
    return summaries


def _find_publish_country_regressions(
    *,
    active_parquet_path: Path,
    candidate_parquet_path: Path,
) -> list[dict[str, str | None]]:
    active_latest = _collect_dataset_country_latest_months(active_parquet_path)
    candidate_latest = _collect_dataset_country_latest_months(candidate_parquet_path)
    regressions: list[dict[str, str | None]] = []
    for country, active_month in active_latest.items():
        candidate_month = candidate_latest.get(country)
        if active_month and not candidate_month:
            regressions.append(
                {
                    "country": country,
                    "activeLatestMonth": active_month,
                    "candidateLatestMonth": None,
                }
            )
            continue
        if (
            active_month
            and candidate_month
            and _time_sort_key(candidate_month) < _time_sort_key(active_month)
        ):
            regressions.append(
                {
                    "country": country,
                    "activeLatestMonth": active_month,
                    "candidateLatestMonth": candidate_month,
                }
            )
    return regressions


def _upload_session_root() -> Path:
    return MONTHLY_UPDATE_JOB_ROOT / "_upload_sessions"


def _upload_session_dir(upload_id: str) -> Path:
    return _upload_session_root() / upload_id


def _upload_session_state_path(upload_id: str) -> Path:
    return _upload_session_dir(upload_id) / UPLOAD_STATE_FILENAME


def _upload_session_chunk_dir(upload_id: str) -> Path:
    return _upload_session_dir(upload_id) / "chunks"


def _upload_session_assembled_path(upload_id: str, filename: str) -> Path:
    return _upload_session_dir(upload_id) / "assembled" / filename


def _iter_upload_session_payloads() -> list[dict[str, Any]]:
    _upload_session_root().mkdir(parents=True, exist_ok=True)
    payloads: list[dict[str, Any]] = []
    for state_path in _upload_session_root().glob(f"*/{UPLOAD_STATE_FILENAME}"):
        try:
            payloads.append(_read_json(state_path))
        except Exception:
            continue
    return payloads


def _list_job_state_payloads() -> list[dict[str, Any]]:
    MONTHLY_UPDATE_JOB_ROOT.mkdir(parents=True, exist_ok=True)
    payloads: list[dict[str, Any]] = []
    for state_path in MONTHLY_UPDATE_JOB_ROOT.glob(f"*/{STATE_FILENAME}"):
        try:
            payloads.append(_read_json(state_path))
        except Exception:
            continue
    return payloads


def _load_upload_session(upload_id: str) -> dict[str, Any]:
    path = _upload_session_state_path(upload_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="上传会话不存在或已失效。")
    return _read_json(path)


def _persist_upload_session(payload: dict[str, Any]) -> None:
    upload_id = str(payload["uploadId"])
    payload["updatedAt"] = _utc_now().isoformat()
    with _WRITE_LOCK:
        _write_json(_upload_session_state_path(upload_id), payload)


def _find_upload_session_by_resume_key(
    *,
    resume_key: str,
    filename: str,
    size_bytes: int,
) -> dict[str, Any] | None:
    if not resume_key:
        return None
    for payload in _iter_upload_session_payloads():
        if str(payload.get("resumeKey", "")) != resume_key:
            continue
        if str(payload.get("filename", "")) != filename:
            continue
        if int(payload.get("sizeBytes", 0) or 0) != size_bytes:
            continue
        if str(payload.get("status", "")) not in {"pending", "uploading", "completed"}:
            continue
        return payload
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def _invalidate_jato_publish_runtime_caches() -> dict[str, Any]:
    result: dict[str, Any] = {
        "marketScanDeckLocal": {"enabled": False, "clearedCount": 0},
        "marketScanDeckRedis": {"enabled": False, "deletedCount": 0},
        "heroProductDeckLocal": {"enabled": False, "clearedCount": 0},
        "heroProductDeckRedis": {"enabled": False, "deletedCount": 0},
        "datasetToken": {
            "enabled": True,
            "message": "Parquet repository dataset token changes with active data artifacts.",
        },
    }
    try:
        from app.infra.redis_client import get_redis_client
        from app.services.hero_product_analysis_service import (
            clear_hero_product_deck_cache,
            invalidate_hero_product_deck_cache,
        )
        from app.services.market_scan_cache import invalidate_market_scan_deck_cache
        from app.services.market_scan_service import clear_market_scan_local_cache

        redis_client = get_redis_client()
        result["marketScanDeckLocal"] = clear_market_scan_local_cache()
        result["marketScanDeckRedis"] = invalidate_market_scan_deck_cache(redis_client)
        result["heroProductDeckLocal"] = clear_hero_product_deck_cache()
        result["heroProductDeckRedis"] = invalidate_hero_product_deck_cache(redis_client)
    except Exception as exc:
        result["error"] = str(exc)
        result["message"] = (
            "Runtime cache invalidation failed; dataset-token cache keys should still "
            "avoid stale MarketScan/HeroProduct data."
        )
    return result


def _write_jato_publish_cache_invalidation_evidence(
    *,
    job_id: str,
    published_at: datetime,
    triggered_by: str,
    cache_invalidation: dict[str, Any],
    active_paths: dict[str, Path],
) -> None:
    ledger_path = PROJECT_ROOT / "hermes" / "evidence_ledger.jsonl"
    stamp = published_at.strftime("%Y%m%dT%H%M%SZ")
    record = {
        "evidenceId": f"evidence.jato_monthly_update.cache_invalidation.{job_id}.{stamp}",
        "evidenceType": "runtime_cache_invalidation",
        "claim": "JATO monthly publish invalidated MarketScan runtime caches.",
        "sourceRef": f"jato_monthly_update_job::{job_id}",
        "artifactId": "feature.jato_monthly_update",
        "confidence": 1.0,
        "supportCount": 1,
        "contradictionCount": 0,
        "createdAt": published_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "details": {
            "publishedBy": triggered_by.strip() or "anonymous",
            "cacheInvalidation": cache_invalidation,
            "activeParquetPath": _relative_to_project(active_paths.get("parquet")),
            "activeManifestPath": _relative_to_project(active_paths.get("manifest")),
            "activePartitionPath": _relative_to_project(active_paths.get("partition")),
        },
    }
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _load_job_state(job_id: str) -> dict[str, Any]:
    path = _job_state_path(job_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="JATO monthly update job not found")
    return _read_json(path)


def _persist_job_state(payload: dict[str, Any]) -> None:
    job_id = str(payload["jobId"])
    payload["updatedAt"] = _utc_now().isoformat()
    with _WRITE_LOCK:
        _write_json(_job_state_path(job_id), payload)


def _thread_is_alive(job_id: str) -> bool:
    worker = _RUNNING_THREADS.get(job_id)
    return bool(worker and worker.is_alive())


def _current_process_pid(payload: dict[str, Any]) -> int | None:
    current = payload.get("currentProcess")
    if not isinstance(current, dict):
        return None
    try:
        pid = int(current.get("pid") or 0)
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _process_exists(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _set_current_process(
    *,
    job_id: str,
    pid: int,
    label: str,
    command: str,
) -> None:
    payload = _load_job_state(job_id)
    now = _utc_now().isoformat()
    payload["currentProcess"] = {
        "pid": pid,
        "label": label,
        "command": command,
        "startedAt": now,
        "lastHeartbeatAt": now,
    }
    _persist_job_state(payload)


def _touch_current_process_heartbeat(
    *,
    job_id: str,
    pid: int,
) -> None:
    payload = _load_job_state(job_id)
    current = payload.get("currentProcess")
    if not isinstance(current, dict) or int(current.get("pid") or 0) != pid:
        return
    current["lastHeartbeatAt"] = _utc_now().isoformat()
    payload["currentProcess"] = current
    _persist_job_state(payload)


def _clear_current_process(
    *,
    job_id: str,
    pid: int | None = None,
) -> None:
    payload = _load_job_state(job_id)
    current = payload.get("currentProcess")
    if pid is not None and isinstance(current, dict) and int(current.get("pid") or 0) != pid:
        return
    payload["currentProcess"] = None
    _persist_job_state(payload)


def _ensure_job_not_cancelled(job_id: str) -> None:
    payload = _load_job_state(job_id)
    if str(payload.get("status") or "") == "cancelled":
        raise _JobCancelled(str(payload.get("error") or f"Job {job_id} cancelled."))


def _infer_job_id_from_log_path(log_path: Path) -> str | None:
    job_id = log_path.parent.name
    return job_id if (_job_state_path(job_id)).exists() else None


def _terminate_process_group(pid: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "pid": pid,
        "sigtermSent": False,
        "sigkillSent": False,
        "processAliveBefore": _process_exists(pid),
        "processAliveAfter": False,
    }
    if not result["processAliveBefore"]:
        return result
    try:
        os.killpg(pid, signal.SIGTERM)
        result["sigtermSent"] = True
    except ProcessLookupError:
        result["processAliveAfter"] = False
        return result
    except OSError as exc:
        result["error"] = str(exc)
        result["processAliveAfter"] = _process_exists(pid)
        return result

    deadline = time.monotonic() + PROCESS_TERMINATE_GRACE_SECONDS
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            result["processAliveAfter"] = False
            return result
        time.sleep(0.2)

    if _process_exists(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
            result["sigkillSent"] = True
        except ProcessLookupError:
            pass
        except OSError as exc:
            result["error"] = str(exc)
    result["processAliveAfter"] = _process_exists(pid)
    return result


def _job_log_probe(log_path: Path) -> dict[str, Any]:
    if not log_path.exists():
        return {
            "path": _relative_to_project(log_path),
            "exists": False,
            "updatedAt": None,
            "ageSeconds": None,
            "stale": True,
        }
    updated = datetime.fromtimestamp(log_path.stat().st_mtime, UTC)
    age_seconds = max(0, int((_utc_now() - updated).total_seconds()))
    return {
        "path": _relative_to_project(log_path),
        "exists": True,
        "updatedAt": updated.isoformat(),
        "ageSeconds": age_seconds,
        "stale": age_seconds > RUNNING_LOG_STALE_SECONDS,
    }


def _artifact_probe(payload: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    probes: list[dict[str, Any]] = []
    for key in ("planPath", "rawCompareReportPath", "refreshReportPath", "stagingOutputPath"):
        value = artifacts.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        path = _project_path(value)
        probes.append({
            "key": key,
            "path": value,
            "exists": bool(path and path.exists()),
        })
    return probes


def _normalize_month(value: str) -> str:
    match = MONTH_PATTERN.fullmatch(value.strip())
    if match is None:
        raise HTTPException(status_code=400, detail="月份格式无效，需要 YYYY-MM，例如 2026-02")
    return f"{match.group(1)}-{int(match.group(2)):02d}"


def _parse_batch_id(value: str | None) -> tuple[str, int] | None:
    candidate = str(value or "").strip()
    match = BATCH_ID_PATTERN.fullmatch(candidate)
    if match is None:
        return None
    return match.group(1), int(match.group(2))


def _infer_month_token(value: str) -> str | None:
    dotted_match = re.findall(
        r"(?<!\d)(20\d{2})[._-](0?[1-9]|1[0-2])(?!\d)", value
    )
    if dotted_match:
        year, month = max(dotted_match)
        return f"{year}-{int(month):02d}"
    compact_match = re.findall(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)", value)
    if compact_match:
        year, month = max(compact_match)
        return f"{year}-{int(month):02d}"
    return None


def _normalize_filename(value: str | None) -> str:
    candidate = Path(value or "jato-update.xlsx").name.strip()
    if not candidate or candidate in {".", ".."}:
        return "jato-update.xlsx"
    return candidate


def _validate_upload(file: UploadFile) -> str:
    return _validate_upload_filename(file.filename or "jato-update.xlsx")


def _validate_upload_filename(filename: str) -> str:
    normalized = _normalize_filename(filename)
    suffix = Path(normalized).suffix.lower()
    if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="JATO monthly update 仅支持 Excel 文件（.xlsx/.xlsm/.xls）。",
        )
    return normalized


def _normalize_size_bytes(value: Any) -> int:
    try:
        size_bytes = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="上传文件大小无效。") from None
    if size_bytes <= 0:
        raise HTTPException(status_code=400, detail="上传文件为空，无法启动月更任务。")
    return size_bytes


def _normalize_sha256(value: Any, *, detail: str) -> str:
    candidate = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", candidate):
        raise HTTPException(status_code=400, detail=detail)
    return candidate


def _sha256_hex_for_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_hex_for_path(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _read_excel_with_fallback(
    input_file: Path,
    *,
    sheet_name: str | int,
    nrows: int | None = None,
    usecols: list[str] | None = None,
) -> pd.DataFrame:
    kwargs: dict[str, Any] = {"sheet_name": sheet_name}
    if nrows is not None:
        kwargs["nrows"] = nrows
    if usecols is not None:
        kwargs["usecols"] = usecols
    try:
        return pd.read_excel(input_file, engine="calamine", **kwargs)
    except Exception:
        try:
            return pd.read_excel(input_file, **kwargs)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"读取上传 Excel 失败：{input_file.name}",
            ) from exc


def _detect_latest_month_from_upload(path: Path) -> str:
    header = _read_excel_with_fallback(
        path,
        sheet_name=DEFAULT_UPLOAD_SHEET_NAME,
        nrows=0,
    )
    header.columns = [str(column).strip() for column in header.columns]
    month_columns = _detect_month_columns(list(header.columns))
    if not month_columns:
        raise HTTPException(
            status_code=400,
            detail="上传文件中未识别到可用月份列，无法自动生成批次。",
        )
    month_frame = _read_excel_with_fallback(
        path,
        sheet_name=DEFAULT_UPLOAD_SHEET_NAME,
        usecols=month_columns,
    )
    month_frame.columns = [str(column).strip() for column in month_frame.columns]
    for column in reversed(month_columns):
        if column in month_frame.columns and _series_has_data(month_frame[column]):
            parsed = datetime.strptime(column.title(), "%Y %b")
            return f"{parsed.year}-{parsed.month:02d}"
    raise HTTPException(
        status_code=400,
        detail="上传文件的月份列均为空，无法自动生成批次。",
    )


def _allocate_batch_id(month: str) -> str:
    normalized_month = _normalize_month(month)
    highest_revision = 0
    for payload in _list_job_state_payloads():
        parsed = _parse_batch_id(payload.get("batchId"))
        if parsed is None:
            continue
        item_month, item_revision = parsed
        if item_month == normalized_month:
            highest_revision = max(highest_revision, item_revision)
    if PATCHES_ROOT.exists():
        for path in PATCHES_ROOT.glob("*"):
            if not path.is_dir():
                continue
            parsed = _parse_batch_id(path.name)
            if parsed is None:
                continue
            item_month, item_revision = parsed
            if item_month == normalized_month:
                highest_revision = max(highest_revision, item_revision)
    return f"{normalized_month}-r{highest_revision + 1}"


def _chunk_file_name(part_number: int) -> str:
    return f"{part_number:05d}.part"


def _expected_chunk_size(*, size_bytes: int, chunk_size: int, total_chunks: int, part_number: int) -> int:
    if part_number < total_chunks:
        return chunk_size
    consumed = chunk_size * (total_chunks - 1)
    return size_bytes - consumed


def _collect_uploaded_chunk_numbers(upload_id: str) -> list[int]:
    chunk_dir = _upload_session_chunk_dir(upload_id)
    if not chunk_dir.exists():
        return []
    chunk_numbers: list[int] = []
    for path in chunk_dir.glob("*.part"):
        try:
            chunk_numbers.append(int(path.stem))
        except ValueError:
            continue
    chunk_numbers.sort()
    return chunk_numbers


def _uploaded_chunk_bytes(upload_id: str) -> int:
    chunk_dir = _upload_session_chunk_dir(upload_id)
    if not chunk_dir.exists():
        return 0
    return sum(
        path.stat().st_size
        for path in chunk_dir.glob("*.part")
        if path.is_file()
    )


def _serialize_upload_session(payload: dict[str, Any]) -> dict[str, Any]:
    upload_id = str(payload["uploadId"])
    persisted_chunks = payload.get("receivedChunks")
    if isinstance(persisted_chunks, list) and persisted_chunks:
        received_chunks = [int(item) for item in persisted_chunks]
    else:
        received_chunks = _collect_uploaded_chunk_numbers(upload_id)
    persisted_uploaded_bytes = payload.get("uploadedBytes")
    uploaded_bytes = (
        int(persisted_uploaded_bytes)
        if persisted_uploaded_bytes is not None
        else _uploaded_chunk_bytes(upload_id)
    )
    return {
        "uploadId": upload_id,
        "filename": str(payload.get("filename", "")),
        "sizeBytes": int(payload.get("sizeBytes", 0)),
        "chunkSize": int(payload.get("chunkSize", UPLOAD_CHUNK_SIZE_BYTES)),
        "totalChunks": int(payload.get("totalChunks", 0)),
        "receivedChunkCount": len(received_chunks),
        "receivedChunks": received_chunks,
        "uploadedBytes": uploaded_bytes,
        "status": str(payload.get("status", "pending")),
        "createdAt": payload.get("createdAt"),
        "updatedAt": payload.get("updatedAt"),
        "completedAt": payload.get("completedAt"),
        "assembledPath": payload.get("assembledPath"),
        "resumeKey": payload.get("resumeKey"),
        "fileSha256": payload.get("fileSha256"),
        "triggeredBy": payload.get("triggeredBy"),
    }


def _ensure_unique_archive_path(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem}-{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _move_to_archive(path: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = _ensure_unique_archive_path(archive_dir / path.name)
    shutil.move(str(path), str(target))
    return target


def _latest_baseline_file(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(
        paths,
        key=lambda item: (
            _infer_month_token(item.stem) or "0000-00",
            item.stat().st_mtime,
            item.name,
        ),
    )


def _list_supported_excel_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.glob("*")
        if path.is_file()
        and path.suffix.lower() in ALLOWED_UPLOAD_EXTENSIONS
        and not path.name.startswith("~$")
    )


def _load_dataset_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame


def _detect_latest_month_from_dataset_frame(
    frame: pd.DataFrame, *, path_label: str
) -> str:
    month_columns = _detect_month_columns(list(frame.columns))
    if not month_columns:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 缺少可识别的月份列，不能生成 baseline。",
        )
    for column in reversed(month_columns):
        if column in frame.columns and _series_has_data(frame[column]):
            parsed = datetime.strptime(column.title(), "%Y %b")
            return f"{parsed.year}-{parsed.month:02d}"
    raise HTTPException(
        status_code=409,
        detail=f"{path_label} 的月份列均为空，不能生成 baseline。",
    )


def _collect_dataset_country_count(frame: pd.DataFrame, *, path_label: str) -> int:
    country_column = _find_country_column(list(frame.columns))
    if country_column is None:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 缺少国家列，不能生成 baseline。",
        )
    values = frame[country_column].astype("string").fillna("").str.strip()
    count = int(values[values != ""].nunique())
    if count <= 0:
        raise HTTPException(
            status_code=409,
            detail=f"{path_label} 不包含有效国家数据，不能生成 baseline。",
        )
    return count


def _baseline_snapshot_filename(*, latest_month: str, country_count: int) -> str:
    year, month = latest_month.split("-")
    jato_month = f"{year}.{int(month)}"
    country_tag = f"full-{country_count}countries" if country_count > 0 else "full"
    return f"JATO-{jato_month}-{country_tag}-baseline.xlsx"


def _measure_path_usage(path: Path) -> tuple[int, int, int]:
    if not path.exists():
        return (0, 0, 0)
    if path.is_file():
        return (int(path.stat().st_size), 1, 0)
    total_bytes = 0
    file_count = 0
    dir_count = 0
    for root, dirnames, filenames in os.walk(path):
        dir_count += len(dirnames)
        for filename in filenames:
            candidate = Path(root) / filename
            try:
                stat = candidate.stat()
            except FileNotFoundError:
                continue
            total_bytes += int(stat.st_size)
            file_count += 1
    return (total_bytes, file_count, dir_count)


def _measure_combined_usage(paths: list[Path]) -> tuple[int, int, int]:
    total_bytes = 0
    file_count = 0
    dir_count = 0
    for path in paths:
        item_bytes, item_files, item_dirs = _measure_path_usage(path)
        total_bytes += item_bytes
        file_count += item_files
        dir_count += item_dirs
    return (total_bytes, file_count, dir_count)


def _build_storage_metric(*, key: str, label: str, paths: list[Path]) -> dict[str, Any]:
    total_bytes, file_count, dir_count = _measure_combined_usage(paths)
    relative_paths = [
        _relative_to_project(path) or str(path)
        for path in paths
        if path.exists()
    ]
    cleanup_tier = {
        "upload-session-cache": SAFE_CLEANUP_TIER,
        "job-upload-copies": SAFE_CLEANUP_TIER,
        "baseline-archive": CAUTIOUS_CLEANUP_TIER,
        "review-reports": CAUTIOUS_CLEANUP_TIER,
        "staging-outputs": CAUTIOUS_CLEANUP_TIER,
        "refresh-backups": CAUTIOUS_CLEANUP_TIER,
    }.get(key, PROTECTED_CLEANUP_TIER)
    return {
        "key": key,
        "label": label,
        "bytes": total_bytes,
        "fileCount": file_count,
        "dirCount": dir_count,
        "paths": relative_paths,
        "cleanupTier": cleanup_tier,
    }


def _normalize_cleanup_tier(value: Any) -> str:
    tier = str(value or SAFE_CLEANUP_TIER).strip().lower() or SAFE_CLEANUP_TIER
    if tier not in ALLOWED_CLEANUP_TIERS:
        supported = ", ".join(sorted(ALLOWED_CLEANUP_TIERS))
        raise HTTPException(
            status_code=400,
            detail=f"不支持的 cleanupTier: {tier}（支持: {supported}）。",
        )
    return tier


def _remove_cleanup_paths(paths: list[Path]) -> tuple[list[str], int]:
    removed_paths: list[str] = []
    freed_bytes = 0
    for path in paths:
        if not path.exists():
            continue
        path_bytes, _, _ = _measure_path_usage(path)
        freed_bytes += path_bytes
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        removed_paths.append(_relative_to_project(path) or str(path))
    return removed_paths, freed_bytes


def _child_cleanup_paths(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.glob("*"))


def _job_upload_dirs() -> list[Path]:
    if not MONTHLY_UPDATE_JOB_ROOT.exists():
        return []
    return sorted(
        [
            path
            for path in MONTHLY_UPDATE_JOB_ROOT.glob("*/uploads")
            if path.is_dir()
        ]
    )


def _resolve_latest_baseline() -> tuple[Path | None, str | None]:
    active_baseline = _latest_baseline_file(_list_supported_excel_files(BASELINE_ROOT))
    if active_baseline is not None:
        return active_baseline, "active"

    archived_root = HISTORY_ARCHIVE_ROOT / "baseline"
    archived_baseline = _latest_baseline_file(_list_supported_excel_files(archived_root))
    if archived_baseline is not None:
        return archived_baseline, "archive"

    return None, None


def _require_latest_baseline() -> tuple[Path, str]:
    baseline_path, baseline_source = _resolve_latest_baseline()
    if baseline_path is None or baseline_source is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "未找到可用 baseline。请先在 01_RAW_DATA/baseline/ 放入当前激活 baseline；"
                "如误清理，可从 01_RAW_DATA/historyDataArchive/baseline/ 恢复后重试。"
            ),
        )
    return baseline_path, baseline_source


def _latest_patch_dir(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(
        paths,
        key=lambda item: (
            _infer_month_token(item.name) or "0000-00",
            item.stat().st_mtime,
            item.name,
        ),
    )


def _append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def _tail_text(path: Path, *, max_lines: int = 160, max_chars: int = 20000) -> str | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail


def _read_text_if_exists(project_relative_path: str | None) -> str | None:
    if not project_relative_path:
        return None
    path = PROJECT_ROOT / project_relative_path
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_flag_value(command_args: list[str], flag: str) -> str | None:
    try:
        index = command_args.index(flag)
    except ValueError:
        return None
    next_index = index + 1
    if next_index >= len(command_args):
        return None
    return command_args[next_index]


def _parse_plan_markdown(plan_path: Path) -> dict[str, Any]:
    text = plan_path.read_text(encoding="utf-8")
    commands = [
        block.strip()
        for block in CODE_BLOCK_PATTERN.findall(text)
        if block.strip()
    ]
    if len(commands) < 2:
        raise RuntimeError("monthly_update_plan.md 缺少 raw compare / refresh 命令")

    metadata: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- 数据月:"):
            metadata["month"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- 批次:"):
            metadata["batchId"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- 对比:"):
            metadata["compareId"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- baseline:"):
            metadata["baselinePath"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- patch:"):
            metadata["patchPath"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- supplement parquet:"):
            metadata["supplementParquetPath"] = stripped.split(":", 1)[1].strip()

    compare_command = commands[0]
    refresh_command = commands[1]
    compare_args = shlex.split(compare_command)
    refresh_args = shlex.split(refresh_command)

    review_dir = _extract_flag_value(compare_args, "--output-dir")
    refresh_report = _extract_flag_value(refresh_args, "--report")
    staging_output = _extract_flag_value(refresh_args, "--output")
    manifest_path = _extract_flag_value(refresh_args, "--manifest")
    partition_output = _extract_flag_value(refresh_args, "--partition-output")
    fingerprint_path = _extract_flag_value(refresh_args, "--fingerprint")

    return {
        "month": metadata.get("month"),
        "batchId": metadata.get("batchId"),
        "compareId": metadata.get("compareId", ""),
        "baselinePath": metadata.get("baselinePath"),
        "patchPath": metadata.get("patchPath"),
        "supplementParquetPath": (
            metadata.get("supplementParquetPath")
            or _extract_flag_value(
                refresh_args,
                "--supplement-missing-countries-from-parquet",
            )
        ),
        "compareCommand": compare_command,
        "refreshCommand": refresh_command,
        "reviewDir": review_dir,
        "rawCompareReportPath": (
            f"{review_dir}/raw_compare_report.json" if review_dir else None
        ),
        "stagingOutputPath": staging_output,
        "manifestPath": manifest_path,
        "partitionOutputPath": partition_output,
        "refreshReportPath": refresh_report,
        "fingerprintPath": fingerprint_path,
    }


def _inject_refresh_supplement_arg(
    refresh_command: str,
) -> tuple[str, str | None]:
    active_parquet_path = _active_data_paths()["parquet"]
    if not active_parquet_path.exists():
        return refresh_command, None

    refresh_args = shlex.split(refresh_command)
    supplement_flag = "--supplement-missing-countries-from-parquet"
    supplement_path = _relative_to_project(active_parquet_path) or str(
        active_parquet_path
    )
    if supplement_flag in refresh_args:
        return (
            refresh_command,
            _extract_flag_value(refresh_args, supplement_flag) or supplement_path,
        )

    refresh_args.extend([supplement_flag, supplement_path])
    return shlex.join(refresh_args), supplement_path


def _command_to_args(command: str) -> list[str]:
    args = shlex.split(command)
    if not args:
        raise RuntimeError("命令为空")
    if args[0] == "python":
        args[0] = sys.executable
    return args


def _read_json_if_exists(project_relative_path: str | None) -> dict[str, Any] | None:
    if not project_relative_path:
        return None
    path = PROJECT_ROOT / project_relative_path
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _summarize_raw_compare_report(report: dict[str, Any]) -> dict[str, Any]:
    findings = report.get("reviewFindings")
    finding_list = findings if isinstance(findings, list) else []
    freshness_entries = report.get("countryFreshnessSummary")
    freshness_list = freshness_entries if isinstance(freshness_entries, list) else []
    scope_summary = report.get("countryScopeSummary")
    scope = scope_summary if isinstance(scope_summary, dict) else {}

    blocker_count = 0
    review_count = 0
    info_count = 0
    for item in finding_list:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity", "")).lower()
        if severity == "blocker":
            blocker_count += 1
        elif severity == "review":
            review_count += 1
        elif severity == "info":
            info_count += 1

    advanced_country_count = 0
    regressed_country_count = 0
    new_country_count = 0
    missing_country_count = 0
    for item in freshness_list:
        if not isinstance(item, dict):
            continue
        status = str(item.get("freshnessStatus", ""))
        if status == "advanced":
            advanced_country_count += 1
        elif status == "regressed":
            regressed_country_count += 1
        elif status == "new_country":
            new_country_count += 1
        elif status == "missing_in_candidate":
            missing_country_count += 1

    added_countries = scope.get("addedCountries")
    removed_countries = scope.get("removedCountries")

    return {
        "compareId": str(report.get("compareId", "")),
        "decisionSuggestion": str(report.get("decisionSuggestion", "")),
        "compareKeyMode": str(report.get("compareKeyMode", "")),
        "compareKeyColumns": (
            [str(item) for item in report.get("compareKeyColumns", [])]
            if isinstance(report.get("compareKeyColumns"), list)
            else []
        ),
        "blockerCount": blocker_count,
        "reviewCount": review_count,
        "infoCount": info_count,
        "advancedCountryCount": advanced_country_count,
        "regressedCountryCount": regressed_country_count,
        "newCountryCount": new_country_count,
        "missingCountryCount": missing_country_count,
        "addedCountryCount": len(added_countries) if isinstance(added_countries, list) else 0,
        "removedCountryCount": (
            len(removed_countries) if isinstance(removed_countries, list) else 0
        ),
    }


def _summarize_refresh_report(report: dict[str, Any]) -> dict[str, Any]:
    full_manifest = report.get("fullManifest")
    full = full_manifest if isinstance(full_manifest, dict) else {}
    partition_manifest = report.get("partitionManifest")
    partition = partition_manifest if isinstance(partition_manifest, dict) else {}
    incremental_payload = report.get("incremental")
    incremental = incremental_payload if isinstance(incremental_payload, dict) else {}
    regression_payload = incremental.get("regression")
    regression = regression_payload if isinstance(regression_payload, dict) else {}
    merge_payload = regression.get("mergeKeyRegression")
    merge = merge_payload if isinstance(merge_payload, dict) else {}

    return {
        "jobStatus": str(report.get("jobStatus", "")),
        "jobElapsedSeconds": float(report.get("jobElapsedSeconds", 0) or 0),
        "rowCount": int(full.get("rows", 0) or 0),
        "columnCount": int(full.get("columns", 0) or 0),
        "partitionCount": int(partition.get("parquetFileCount", 0) or 0),
        "changedRows": int(regression.get("changedRows", 0) or 0),
        "changedCountryCount": int(regression.get("changedCountryCount", 0) or 0),
        "fingerprintMatched": bool(incremental.get("fingerprintMatched")),
        "fingerprintUpdated": bool(incremental.get("fingerprintUpdated")),
        "conflictGroupCount": int(merge.get("conflictGroupCount", 0) or 0),
        "conflictRowCount": int(merge.get("conflictRowCount", 0) or 0),
    }


def _parse_status_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    except (TypeError, ValueError):
        return None


def _status_duration_seconds(started_at: Any, finished_at: Any) -> int:
    started = _parse_status_dt(started_at)
    finished = _parse_status_dt(finished_at)
    if not started or not finished:
        return 0
    return max(0, int((finished - started).total_seconds()))


def _jato_etl_artifact_refs(state: dict[str, Any]) -> list[str]:
    artifacts = state.get("artifacts") if isinstance(state.get("artifacts"), dict) else {}
    refs: list[str] = []
    for key in (
        "logPath",
        "planPath",
        "rawCompareReportPath",
        "stagingOutputPath",
        "manifestPath",
        "partitionOutputPath",
        "refreshReportPath",
        "fingerprintPath",
    ):
        value = artifacts.get(key)
        if isinstance(value, str) and value.strip():
            refs.append(value.strip())
    return refs


def _write_jato_etl_pipeline_status(state: dict[str, Any]) -> None:
    """Publish JATO monthly update runtime status for Hermes Sentinel."""
    try:
        summaries = state.get("summaries") if isinstance(state.get("summaries"), dict) else {}
        refresh = summaries.get("refresh") if isinstance(summaries.get("refresh"), dict) else {}
        job_status = str(state.get("status") or "unknown").strip().lower()
        status = (
            "success"
            if job_status == "success"
            else "failed"
            if job_status == "failed"
            else "degraded"
            if job_status == "cancelled"
            else "unknown"
        )
        message = str(state.get("error") or "").strip()
        if not message:
            message = (
                f"jobId={state.get('jobId', '')} "
                f"month={state.get('month', '')} phase={state.get('phase', '')}"
            ).strip()

        write_pipeline_status({
            "pipelineId": "jato_etl",
            "status": status,
            "lastRunAt": state.get("finishedAt") or state.get("updatedAt") or state.get("startedAt"),
            "startedAt": state.get("startedAt"),
            "finishedAt": state.get("finishedAt"),
            "exitCode": 0 if status == "success" else 1 if status == "failed" else 130 if status == "degraded" else None,
            "durationSeconds": _status_duration_seconds(state.get("startedAt"), state.get("finishedAt")),
            "recordsProcessed": refresh.get("rowCount", 0),
            "failedCount": 1 if status == "failed" else 0,
            "warningCount": 1 if status == "degraded" else 0,
            "artifactRefs": _jato_etl_artifact_refs(state),
            "source": "app.services.jato_monthly_update_service",
            "message": message,
            "jobId": state.get("jobId"),
            "month": state.get("month"),
            "batchId": state.get("batchId"),
            "phase": state.get("phase"),
            "triggeredBy": state.get("triggeredBy"),
        })
    except Exception:
        return


def _serialize_job_state(
    payload: dict[str, Any],
    *,
    include_log_tail: bool,
) -> dict[str, Any]:
    item = {
        "jobId": str(payload.get("jobId", "")),
        "month": (
            None
            if payload.get("month") in {None, ""}
            else str(payload.get("month"))
        ),
        "batchId": (
            None
            if payload.get("batchId") in {None, ""}
            else str(payload.get("batchId"))
        ),
        "status": str(payload.get("status", "")),
        "phase": str(payload.get("phase", "")),
        "jobType": payload.get("jobType"),
        "country": payload.get("country"),
        "countryScope": (
            [str(country) for country in payload.get("countryScope", [])]
            if isinstance(payload.get("countryScope"), list)
            else []
        ),
        "triggeredBy": str(payload.get("triggeredBy", "")),
        "createdAt": str(payload.get("createdAt", "")),
        "updatedAt": str(payload.get("updatedAt", "")),
        "startedAt": payload.get("startedAt"),
        "finishedAt": payload.get("finishedAt"),
        "error": payload.get("error"),
        "upload": payload.get("upload") if isinstance(payload.get("upload"), dict) else None,
        "plan": payload.get("plan") if isinstance(payload.get("plan"), dict) else None,
        "artifacts": (
            payload.get("artifacts")
            if isinstance(payload.get("artifacts"), dict)
            else None
        ),
        "summaries": (
            payload.get("summaries")
            if isinstance(payload.get("summaries"), dict)
            else None
        ),
        "logPath": payload.get("logPath"),
        "publication": (
            payload.get("publication")
            if isinstance(payload.get("publication"), dict)
            else None
        ),
        "currentProcess": (
            payload.get("currentProcess")
            if isinstance(payload.get("currentProcess"), dict)
            else None
        ),
        "runtimeCheck": (
            payload.get("runtimeCheck")
            if isinstance(payload.get("runtimeCheck"), dict)
            else None
        ),
        "cancellation": (
            payload.get("cancellation")
            if isinstance(payload.get("cancellation"), dict)
            else None
        ),
        "reviewApproval": (
            payload.get("reviewApproval")
            if isinstance(payload.get("reviewApproval"), dict)
            else None
        ),
    }
    if include_log_tail:
        log_path = _job_log_path(item["jobId"])
        item["logTail"] = _tail_text(log_path)
    return item


def _sanitize_review_finding(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    metrics = item.get("metrics")
    return {
        "severity": str(item.get("severity", "")),
        "scope": str(item.get("scope", "")),
        "target": str(item.get("target", "")),
        "ruleId": str(item.get("ruleId", "")),
        "message": str(item.get("message", "")),
        "metrics": metrics if isinstance(metrics, dict) else {},
        "suggestedAction": str(item.get("suggestedAction", "")),
        "sourceFeedback": (
            None
            if item.get("sourceFeedback") in {None, ""}
            else str(item.get("sourceFeedback"))
        ),
    }


def _sanitize_conflict_sample(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    business_key = item.get("businessKey")
    changed_fields = item.get("changedFields")
    return {
        "country": str(item.get("country", "")),
        "businessKey": business_key if isinstance(business_key, dict) else {},
        "oldValueDigest": (
            None
            if item.get("oldValueDigest") in {None, ""}
            else str(item.get("oldValueDigest"))
        ),
        "newValueDigest": (
            None
            if item.get("newValueDigest") in {None, ""}
            else str(item.get("newValueDigest"))
        ),
        "changedFields": (
            [str(field) for field in changed_fields]
            if isinstance(changed_fields, list)
            else []
        ),
    }


def _sanitize_overlap_change_summary(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    return {
        "country": str(item.get("country", "")),
        "compareMonths": (
            [str(month) for month in item.get("compareMonths", [])]
            if isinstance(item.get("compareMonths"), list)
            else []
        ),
        "compareKeyColumns": (
            [str(column) for column in item.get("compareKeyColumns", [])]
            if isinstance(item.get("compareKeyColumns"), list)
            else []
        ),
        "addedRecordCount": int(item.get("addedRecordCount", 0) or 0),
        "removedRecordCount": int(item.get("removedRecordCount", 0) or 0),
        "changedRecordCount": int(item.get("changedRecordCount", 0) or 0),
        "unchangedRecordCount": int(item.get("unchangedRecordCount", 0) or 0),
        "changeRate": float(item.get("changeRate", 0) or 0),
        "sampleAddedKeys": (
            [entry for entry in item.get("sampleAddedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleAddedKeys"), list)
            else []
        ),
        "sampleRemovedKeys": (
            [entry for entry in item.get("sampleRemovedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleRemovedKeys"), list)
            else []
        ),
        "sampleChangedKeys": (
            [entry for entry in item.get("sampleChangedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleChangedKeys"), list)
            else []
        ),
    }


def _sanitize_country_freshness_summary(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    return {
        "country": str(item.get("country", "")),
        "oldLatestMonth": (
            None
            if item.get("oldLatestMonth") in {None, ""}
            else str(item.get("oldLatestMonth"))
        ),
        "newLatestMonth": (
            None
            if item.get("newLatestMonth") in {None, ""}
            else str(item.get("newLatestMonth"))
        ),
        "freshnessStatus": str(item.get("freshnessStatus", "")),
        "rowDelta": int(item.get("rowDelta", 0) or 0),
    }


def _sanitize_country_coverage_summary(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    return {
        "country": str(item.get("country", "")),
        "oldMonths": (
            [str(month) for month in item.get("oldMonths", [])]
            if isinstance(item.get("oldMonths"), list)
            else []
        ),
        "newMonths": (
            [str(month) for month in item.get("newMonths", [])]
            if isinstance(item.get("newMonths"), list)
            else []
        ),
        "addedMonths": (
            [str(month) for month in item.get("addedMonths", [])]
            if isinstance(item.get("addedMonths"), list)
            else []
        ),
        "removedMonths": (
            [str(month) for month in item.get("removedMonths", [])]
            if isinstance(item.get("removedMonths"), list)
            else []
        ),
        "overlappingMonths": (
            [str(month) for month in item.get("overlappingMonths", [])]
            if isinstance(item.get("overlappingMonths"), list)
            else []
        ),
        "coverageStatus": str(item.get("coverageStatus", "")),
    }


def _require_no_running_monthly_update_jobs(*, excluding_job_id: str | None = None) -> None:
    running_jobs = [
        str(payload.get("jobId", ""))
        for payload in _list_job_state_payloads()
        if str(payload.get("status", "")) in {"queued", "running"}
        and str(payload.get("jobId", "")) != str(excluding_job_id or "")
    ]
    if running_jobs:
        raise HTTPException(
            status_code=409,
            detail="存在运行中的月更任务，请等待完成后再执行 review / publish。",
        )


def _build_single_country_review(payload: dict[str, Any]) -> dict[str, Any]:
    country = str(payload.get("country") or "").strip()
    artifacts = payload.get("artifacts")
    if not country or not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="目标国家任务缺少 Review 所需信息。")
    candidate_path = _project_path(str(artifacts.get("stagingOutputPath") or "").strip())
    active_paths = _active_data_paths()
    if candidate_path is None or not candidate_path.exists():
        raise HTTPException(status_code=409, detail="单国任务尚未生成可 Review 的 candidate。")
    candidate_frame = _load_parquet_country_subset(candidate_path, country, path_label="candidate")
    if candidate_frame.empty:
        raise HTTPException(status_code=409, detail="单国 candidate 不包含目标国家。")
    if active_paths["partition"].exists():
        active_frame = _load_active_country_partition_subset(active_paths["partition"], country)
    elif active_paths["parquet"].exists():
        active_frame = _load_parquet_country_subset(active_paths["parquet"], country, path_label="active")
    else:
        raise HTTPException(status_code=409, detail="缺少 active 数据，不能生成单国 Review。")
    if active_frame.empty:
        raise HTTPException(status_code=409, detail="active 不包含目标国家，不能生成单国 Review。")

    month_columns = _detect_month_columns(list(candidate_frame.columns))
    schema_contract = _single_country_schema_contract(
        active_frame=active_frame,
        candidate_frame=candidate_frame,
    )
    key_columns = _single_country_configuration_key_columns(candidate_frame)
    duplicate_count = int(candidate_frame.duplicated(subset=key_columns, keep=False).sum()) if key_columns else 0
    negative_sales_count = int(sum(
        pd.to_numeric(candidate_frame[column], errors="coerce").lt(0).sum()
        for column in month_columns
    ))
    active_latest = _latest_month_from_frame(active_frame)
    candidate_latest = _latest_month_from_frame(candidate_frame)
    historical_stability = _single_country_historical_sales_stability(
        active_frame=active_frame,
        candidate_frame=candidate_frame,
        active_latest_month=active_latest,
    )
    active_sales = _collect_country_monthly_sales(active_frame, countries=[country], path_label="active").get(country, {})
    candidate_sales = _collect_country_monthly_sales(candidate_frame, countries=[country], path_label="candidate").get(country, {})
    common_months = sorted(set(active_sales) & set(candidate_sales), key=_time_sort_key)
    doubled_months = [
        month for month in common_months
        if _is_near_sales_doubling(
            reference_sales=active_sales.get(month),
            candidate_sales=candidate_sales.get(month),
        )[0]
    ]
    partition_check = artifacts.get("untouchedPartitionCheck")
    if not isinstance(partition_check, dict):
        partition_check = {"status": "unavailable", "changedPartitions": []}

    findings: list[dict[str, Any]] = []

    def add_finding(severity: str, rule_id: str, message: str, metrics: dict[str, Any]) -> None:
        finding: dict[str, Any] = {
            "severity": severity,
            "scope": "country",
            "target": country,
            "ruleId": rule_id,
            "message": message,
            "metrics": metrics,
            "suggestedAction": "reject_input_batch" if severity == "blocker" else "manual_review_required",
        }
        source_feedback = _single_country_source_feedback(
            rule_id=rule_id,
            country=country,
            metrics=metrics,
        )
        if source_feedback:
            finding["sourceFeedback"] = source_feedback
        findings.append(finding)

    if not month_columns:
        add_finding("blocker", "SC001", "candidate 缺少月份列。", {})
    if schema_contract["missingMaterial"]:
        add_finding("blocker", "SC009", "目标国家 candidate 缺少 active 业务列：" + "、".join(schema_contract["missingMaterial"]) + "。", {
            "missingMaterialColumns": schema_contract["missingMaterial"],
            "missingDerivedYtdColumns": schema_contract["missingDerivedYtd"],
            "missingNullOnlyColumns": schema_contract["missingNullOnly"],
        })
    if schema_contract["missingDeprecatedOptional"]:
        add_finding(
            "review",
            "SC014",
            "目标国家 candidate 缺少已停用的可选静态字段："
            + "、".join(schema_contract["missingDeprecatedOptional"])
            + "。Smart Merge 仅在配置键匹配且 active 旧值一致时沿用旧值。",
            {
                "missingDeprecatedOptionalColumns": schema_contract[
                    "missingDeprecatedOptional"
                ],
                "activePopulatedRowsByColumn": {
                    column: int(
                        (~_series_missing_value_mask(active_frame[column])).sum()
                    )
                    for column in schema_contract["missingDeprecatedOptional"]
                },
                "carryForwardPolicy": "consistent_active_key_values_only",
                "unmatchedPolicy": "leave_null",
            },
        )
    if schema_contract["missingDerivedYtd"]:
        add_finding("review", "SC013", "目标国家 candidate 缺少旧月份 YTD 派生列：" + "、".join(schema_contract["missingDerivedYtd"]) + "。", {
            "missingDerivedYtdColumns": schema_contract["missingDerivedYtd"],
        })
    if schema_contract["extra"]:
        add_finding("review", "SC010", "目标国家 candidate 包含 active 中没有的新业务列：" + "、".join(schema_contract["extra"]) + "。", {
            "extraColumns": schema_contract["extra"],
        })
    if candidate_latest is None:
        add_finding("blocker", "SC002", "candidate 没有有效销量月份。", {})
    if active_latest and candidate_latest and _time_sort_key(candidate_latest) < _time_sort_key(active_latest):
        add_finding("blocker", "SC003", "目标国家最新月份发生回退。", {"active": active_latest, "candidate": candidate_latest})
    if duplicate_count:
        add_finding("blocker", "SC004", "目标国家 candidate 存在完全相同的配置指纹。", {
            "duplicateRows": duplicate_count,
            "keyColumnCount": len(key_columns),
        })
    if negative_sales_count:
        add_finding("blocker", "SC005", "目标国家 candidate 存在负销量。", {"negativeSalesCells": negative_sales_count})
    if len(doubled_months) >= SALES_DOUBLING_MIN_MONTH_COUNT:
        add_finding("blocker", "SC006", "目标国家 candidate 疑似销量翻倍。", {"months": doubled_months})
    if historical_stability.get("status") == "fail":
        add_finding("blocker", "SC011", "目标国家 candidate 改写了 active 已有历史销量。", historical_stability)
    row_delta = int(len(candidate_frame) - len(active_frame))
    if row_delta:
        add_finding("review", "SC012", "目标国家 candidate 的配置行数与 active 不同。", {
            "activeRows": int(len(active_frame)),
            "candidateRows": int(len(candidate_frame)),
            "rowDelta": row_delta,
            "historicalSalesStability": historical_stability,
        })
    if partition_check.get("status") == "fail":
        add_finding("blocker", "SC007", "未上传国家的分区签名发生变化。", partition_check)
    elif partition_check.get("status") == "unavailable":
        add_finding("review", "SC008", "无法验证未上传国家分区稳定性。", partition_check)
    findings.append({
        "severity": "info",
        "scope": "country",
        "target": country,
        "ruleId": "SC201",
        "message": "candidate 仅读取目标国家分区；未上传国家 active 分区保持只读。",
        "metrics": {"rowCount": int(len(candidate_frame)), "latestMonth": candidate_latest},
        "suggestedAction": "manual_review_required",
    })

    rows = []
    for month in sorted(set(active_sales) | set(candidate_sales), key=_time_sort_key):
        reference_sales = active_sales.get(month)
        proposed_sales = candidate_sales.get(month)
        rows.append({
            "month": month,
            "referenceSales": reference_sales,
            "candidateSales": proposed_sales,
            "deltaSales": _serialize_numeric_value((proposed_sales or 0) - (reference_sales or 0)) if month in active_sales and month in candidate_sales else None,
            "changeStatus": "unchanged" if reference_sales == proposed_sales else "changed",
        })
    return {
        "jobId": str(payload.get("jobId") or ""),
        "reviewDir": None,
        "compareId": f"{payload.get('jobId')}-single-country",
        "decisionSuggestion": "reject_input_batch" if any(item["severity"] == "blocker" for item in findings) else "manual_review_required",
        "compareKeyColumns": key_columns,
        "checklistMarkdown": "\n".join(f"- {item['severity']}: {item['message']}" for item in findings),
        "reviewFindings": findings,
        "sampledCountries": [country],
        "conflictSampleCount": 0,
        "conflictSamples": [],
        "overlapChangeSummary": [],
        "countryFreshnessSummary": [{
            "country": country,
            "oldLatestMonth": active_latest,
            "newLatestMonth": candidate_latest,
            "freshnessStatus": "advanced" if active_latest and candidate_latest and _time_sort_key(candidate_latest) > _time_sort_key(active_latest) else "unchanged_latest",
            "rowDelta": row_delta,
        }],
        "countryCoverageSummary": [{
            "country": country,
            "oldMonths": _detect_month_columns(list(active_frame.columns)),
            "newMonths": month_columns,
            "addedMonths": sorted(set(month_columns) - set(_detect_month_columns(list(active_frame.columns))), key=_time_sort_key),
            "removedMonths": sorted(set(_detect_month_columns(list(active_frame.columns))) - set(month_columns), key=_time_sort_key),
            "overlappingMonths": common_months,
            "coverageStatus": "single_country",
        }],
        "countrySalesReferenceLabel": "网站当前 active",
        "countryMonthlySalesSummary": [{"country": country, "rows": rows}],
        "countryMonthlySalesError": None,
        "timeAxisCheck": {
            "targetCountry": country,
            "activeLatestMonth": active_latest,
            "candidateLatestMonth": candidate_latest,
            "schema": schema_contract,
        },
        "countryScopeSummary": {"targetCountry": country, "untouchedPartitionCheck": partition_check},
        "refreshSummary": _summarize_refresh_report(_read_json_if_exists(str(artifacts.get("refreshReportPath") or "")) or {}),
        "candidateFingerprint": _candidate_fingerprint_id(artifacts),
        "approval": payload.get("reviewApproval"),
    }


def _build_partial_country_review(payload: dict[str, Any]) -> dict[str, Any]:
    countries = _ordered_distinct_strings(
        [
            str(country)
            for country in (
                payload.get("countryScope")
                if isinstance(payload.get("countryScope"), list)
                else []
            )
        ]
    )
    if len(countries) < 2:
        raise HTTPException(status_code=409, detail="部分国家任务缺少至少两个目标国家。")

    country_reviews = [
        _build_single_country_review({**payload, "country": country})
        for country in countries
    ]
    findings = [
        finding
        for review in country_reviews
        for finding in review["reviewFindings"]
    ]
    compare_key_columns = _ordered_distinct_strings(
        [
            column
            for review in country_reviews
            for column in review["compareKeyColumns"]
        ]
    )
    checklist_sections = [
        "\n".join(
            [
                f"## {country}",
                review["checklistMarkdown"],
            ]
        )
        for country, review in zip(countries, country_reviews, strict=True)
    ]
    artifacts = payload.get("artifacts")
    partition_check = (
        artifacts.get("untouchedPartitionCheck")
        if isinstance(artifacts, dict)
        else None
    )
    if not isinstance(partition_check, dict):
        partition_check = {"status": "unavailable"}
    return {
        "jobId": str(payload.get("jobId") or ""),
        "reviewDir": None,
        "compareId": f"{payload.get('jobId')}-partial-country",
        "decisionSuggestion": (
            "reject_input_batch"
            if any(item["severity"] == "blocker" for item in findings)
            else "manual_review_required"
        ),
        "compareKeyColumns": compare_key_columns,
        "checklistMarkdown": "\n\n".join(checklist_sections),
        "reviewFindings": findings,
        "sampledCountries": countries,
        "conflictSampleCount": 0,
        "conflictSamples": [],
        "overlapChangeSummary": [
            item
            for review in country_reviews
            for item in review["overlapChangeSummary"]
        ],
        "countryFreshnessSummary": [
            item
            for review in country_reviews
            for item in review["countryFreshnessSummary"]
        ],
        "countryCoverageSummary": [
            item
            for review in country_reviews
            for item in review["countryCoverageSummary"]
        ],
        "countrySalesReferenceLabel": "网站当前 active",
        "countryMonthlySalesSummary": [
            item
            for review in country_reviews
            for item in review["countryMonthlySalesSummary"]
        ],
        "countryMonthlySalesError": None,
        "timeAxisCheck": {
            "countries": {
                country: review["timeAxisCheck"]
                for country, review in zip(countries, country_reviews, strict=True)
            },
        },
        "countryScopeSummary": {
            "targetCountries": countries,
            "untouchedPartitionCheck": partition_check,
        },
        "refreshSummary": country_reviews[0]["refreshSummary"],
        "candidateFingerprint": country_reviews[0]["candidateFingerprint"],
        "approval": payload.get("reviewApproval"),
    }


def get_jato_monthly_update_review(job_id: str) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务暂无可 review 的 compare 产物。")

    raw_compare_report_path = str(artifacts.get("rawCompareReportPath") or "").strip()
    review_dir = str(artifacts.get("reviewDir") or "").strip()
    raw_compare_report = _read_json_if_exists(raw_compare_report_path)
    if raw_compare_report is None:
        job_type = str(payload.get("jobType") or "")
        if job_type == "single_country":
            return _build_single_country_review(payload)
        if job_type == "partial_country":
            return _build_partial_country_review(payload)
        raise HTTPException(status_code=409, detail="当前任务暂无可 review 的 compare 报告。")

    checklist_markdown = _read_text_if_exists(
        f"{review_dir}/review_checklist.md" if review_dir else None
    )
    conflict_payload = _read_json_if_exists(
        f"{review_dir}/conflict_samples.json" if review_dir else None
    )
    refresh_report = _read_json_if_exists(str(artifacts.get("refreshReportPath") or "").strip())

    findings = [
        sanitized
        for sanitized in (
            _sanitize_review_finding(item)
            for item in raw_compare_report.get("reviewFindings", [])
        )
        if sanitized is not None
    ]
    sampled_countries: list[str] = []
    sample_count = 0
    conflict_samples: list[dict[str, Any]] = []
    overlap_change_summary = [
        sanitized
        for sanitized in (
            _sanitize_overlap_change_summary(item)
            for item in raw_compare_report.get("overlapChangeSummary", [])
        )
        if sanitized is not None
    ]
    country_freshness_summary = [
        sanitized
        for sanitized in (
            _sanitize_country_freshness_summary(item)
            for item in raw_compare_report.get("countryFreshnessSummary", [])
        )
        if sanitized is not None
    ]
    country_coverage_summary = [
        sanitized
        for sanitized in (
            _sanitize_country_coverage_summary(item)
            for item in raw_compare_report.get("countryCoverageSummary", [])
        )
        if sanitized is not None
    ]
    if isinstance(conflict_payload, dict):
        sampled = conflict_payload.get("sampledCountries")
        samples = conflict_payload.get("samples")
        if isinstance(sampled, list):
            sampled_countries = [str(item) for item in sampled]
        if isinstance(samples, list):
            sample_count = len(samples)
            conflict_samples = [
                sanitized
                for sanitized in (
                    _sanitize_conflict_sample(item) for item in samples
                )
                if sanitized is not None
            ]

    review_countries = _ordered_distinct_strings(
        [
            *(item.get("country", "") for item in overlap_change_summary),
            *(item.get("country", "") for item in country_freshness_summary),
            *(item.get("country", "") for item in country_coverage_summary),
            *sampled_countries,
        ]
    )
    country_monthly_sales_summary: list[dict[str, Any]] = []
    country_sales_reference_label = "-"
    country_monthly_sales_error: str | None = None
    candidate_path = _project_path(str(artifacts.get("stagingOutputPath") or "").strip())
    if review_countries:
        if candidate_path is None:
            country_monthly_sales_error = (
                "缺少 candidate parquet 产物，无法生成逐月销量核对表。"
            )
        else:
            reference_path, country_sales_reference_label = (
                _resolve_review_reference_dataset(artifacts)
            )
            try:
                country_monthly_sales_summary = _build_country_monthly_sales_summary(
                    countries=review_countries,
                    candidate_path=candidate_path,
                    reference_path=reference_path,
                )
            except HTTPException as exc:
                country_monthly_sales_error = str(exc.detail)

    return {
        "jobId": job_id,
        "reviewDir": review_dir or None,
        "compareId": str(raw_compare_report.get("compareId", "")),
        "decisionSuggestion": str(raw_compare_report.get("decisionSuggestion", "")),
        "compareKeyColumns": (
            [str(item) for item in raw_compare_report.get("compareKeyColumns", [])]
            if isinstance(raw_compare_report.get("compareKeyColumns"), list)
            else []
        ),
        "checklistMarkdown": checklist_markdown,
        "reviewFindings": findings,
        "sampledCountries": sampled_countries,
        "conflictSampleCount": sample_count,
        "conflictSamples": conflict_samples,
        "overlapChangeSummary": overlap_change_summary,
        "countryFreshnessSummary": country_freshness_summary,
        "countryCoverageSummary": country_coverage_summary,
        "countrySalesReferenceLabel": country_sales_reference_label,
        "countryMonthlySalesSummary": country_monthly_sales_summary,
        "countryMonthlySalesError": country_monthly_sales_error,
        "timeAxisCheck": (
            raw_compare_report.get("timeAxisCheck")
            if isinstance(raw_compare_report.get("timeAxisCheck"), dict)
            else {}
        ),
        "countryScopeSummary": (
            raw_compare_report.get("countryScopeSummary")
            if isinstance(raw_compare_report.get("countryScopeSummary"), dict)
            else {}
        ),
        "refreshSummary": (
            _summarize_refresh_report(refresh_report)
            if isinstance(refresh_report, dict)
            else None
        ),
        "candidateFingerprint": _candidate_fingerprint_id(artifacts),
        "approval": payload.get("reviewApproval"),
    }


def approve_jato_monthly_update_review(
    *,
    job_id: str,
    triggered_by: str,
    decision: str,
    note: str | None = None,
) -> dict[str, Any]:
    normalized_decision = decision.strip().lower()
    if normalized_decision not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="review decision 只支持 approve 或 reject。")
    payload = _load_job_state(job_id)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务缺少 candidate，不能确认 Review。")
    review = get_jato_monthly_update_review(job_id)
    findings = review.get("reviewFindings")
    has_blocker = isinstance(findings, list) and any(
        isinstance(item, dict) and item.get("severity") == "blocker"
        for item in findings
    )
    if normalized_decision == "approve" and has_blocker:
        raise HTTPException(status_code=409, detail="Review 存在 blocker，不能批准 Publish。")
    payload["reviewApproval"] = {
        "decision": "approved" if normalized_decision == "approve" else "rejected",
        "reviewedAt": _utc_now().isoformat(),
        "reviewedBy": triggered_by.strip() or "anonymous",
        "candidateFingerprint": _candidate_fingerprint_id(artifacts),
        "note": str(note or "").strip() or None,
    }
    _persist_job_state(payload)
    _append_log(
        _job_log_path(job_id),
        f"[{_utc_now().isoformat()}] Review {normalized_decision} by {triggered_by.strip() or 'anonymous'}.",
    )
    return _serialize_job_state(payload, include_log_tail=True)


def publish_jato_monthly_update_job(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    _require_no_running_monthly_update_jobs(excluding_job_id=job_id)
    payload = _load_job_state(job_id)
    if str(payload.get("status", "")) != "success" or str(payload.get("phase", "")) != "completed":
        raise HTTPException(
            status_code=409,
            detail="只有 success/completed 的月更任务才能执行 publish。",
        )

    publication = payload.get("publication")
    if (
        isinstance(publication, dict)
        and publication.get("publishedAt")
        and not publication.get("rolledBackAt")
    ):
        raise HTTPException(status_code=409, detail="该月更任务已经 publish 过。")

    summaries = payload.get("summaries")
    refresh_summary = summaries.get("refresh") if isinstance(summaries, dict) else None
    if not isinstance(refresh_summary, dict) or str(refresh_summary.get("jobStatus", "")) != "success":
        raise HTTPException(status_code=409, detail="当前任务 refresh 尚未成功，不能 publish。")

    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务缺少 staging 产物信息，不能 publish。")
    if artifacts.get("candidateScope") in {
        "target_country_partition_only",
        "target_country_partitions_only",
    }:
        raise HTTPException(
            status_code=409,
            detail="部分国家 candidate 仅用于 Review，不能直接 Publish。请使用已批准的完整 promotion 流程生成 active 产物。",
        )
    approval = payload.get("reviewApproval")
    if not isinstance(approval, dict) or approval.get("decision") != "approved":
        raise HTTPException(status_code=409, detail="必须先完成并批准当前 candidate 的 Review，才能 publish。")
    if str(approval.get("candidateFingerprint") or "") != _candidate_fingerprint_id(artifacts):
        raise HTTPException(status_code=409, detail="candidate 在 Review 后已变化，请重新 Review 并批准。")

    source_paths = {
        "parquet": _project_path(str(artifacts.get("stagingOutputPath") or "").strip()),
        "manifest": _project_path(str(artifacts.get("manifestPath") or "").strip()),
        "partition": _project_path(str(artifacts.get("partitionOutputPath") or "").strip()),
        "fingerprint": _project_path(str(artifacts.get("fingerprintPath") or "").strip()),
        "refreshReport": _project_path(str(artifacts.get("refreshReportPath") or "").strip()),
    }
    missing_sources = [
        key
        for key, path in source_paths.items()
        if path is None or not path.exists()
    ]
    if missing_sources:
        raise HTTPException(
            status_code=409,
            detail=f"当前任务缺少 publish 所需产物：{', '.join(missing_sources)}。",
        )

    active_paths = _active_data_paths()
    if active_paths["parquet"].exists():
        regressions = _find_publish_country_regressions(
            active_parquet_path=active_paths["parquet"],
            candidate_parquet_path=source_paths["parquet"],
        )
        if regressions:
            rendered = ", ".join(
                (
                    f"{entry['country']} "
                    f"({entry['activeLatestMonth']} -> {entry['candidateLatestMonth'] or '-'})"
                )
                for entry in regressions[:5]
            )
            extra = " 等" if len(regressions) > 5 else ""
            raise HTTPException(
                status_code=409,
                detail={
                    "blockerType": "country_regression",
                    "message": (
                        "publish 会让当前 active 数据回退，请先更换 baseline 或重建 candidate："
                        f"{rendered}{extra}"
                    ),
                    "regressions": [
                        {
                            "country": r["country"],
                            "activeLatestMonth": r["activeLatestMonth"],
                            "candidateLatestMonth": r["candidateLatestMonth"],
                        }
                        for r in regressions
                    ],
                },
            )
        sales_doubling_anomalies = _find_publish_sales_doubling_anomalies(
            active_parquet_path=active_paths["parquet"],
            candidate_parquet_path=source_paths["parquet"],
        )
        if sales_doubling_anomalies:
            rendered = _render_sales_doubling_anomalies(sales_doubling_anomalies)
            raise HTTPException(
                status_code=409,
                detail={
                    "blockerType": "sales_doubling",
                    "message": (
                        "publish 检测到 candidate 疑似重复合并，多个重叠月份销量约为 "
                        f"当前 active 的 2x：{rendered}。请先重建 candidate 或回滚到正确 active。"
                    ),
                    "anomalies": [
                        {
                            "country": a["country"],
                            "suspiciousMonthCount": a["suspiciousMonthCount"],
                            "sampleMonths": a["sampleMonths"][:3],
                            "rolling12Ratio": a["rolling12Ratio"],
                        }
                        for a in sales_doubling_anomalies
                    ],
                },
            )

    published_at = _utc_now()
    backup_dir = active_paths["backupRoot"] / (
        f"manual-promote-{job_id}-{published_at.strftime('%Y%m%d-%H%M%S')}"
    )
    backup_dir.mkdir(parents=True, exist_ok=True)

    if active_paths["parquet"].exists():
        shutil.copy2(active_paths["parquet"], backup_dir / active_paths["parquet"].name)
    if active_paths["manifest"].exists():
        shutil.copy2(active_paths["manifest"], backup_dir / active_paths["manifest"].name)
    if active_paths["fingerprint"].exists():
        shutil.copy2(active_paths["fingerprint"], backup_dir / active_paths["fingerprint"].name)
    if active_paths["refreshReport"].exists():
        shutil.copy2(active_paths["refreshReport"], backup_dir / active_paths["refreshReport"].name)
    if active_paths["partition"].exists():
        shutil.copytree(
            active_paths["partition"],
            backup_dir / active_paths["partition"].name,
        )

    shutil.copy2(source_paths["parquet"], active_paths["parquet"])
    shutil.copy2(source_paths["manifest"], active_paths["manifest"])
    shutil.copy2(source_paths["fingerprint"], active_paths["fingerprint"])
    shutil.copy2(source_paths["refreshReport"], active_paths["refreshReport"])
    if active_paths["partition"].exists():
        shutil.rmtree(active_paths["partition"])
    shutil.copytree(source_paths["partition"], active_paths["partition"])

    cache_invalidation = _invalidate_jato_publish_runtime_caches()
    payload["publication"] = {
        "publishedAt": published_at.isoformat(),
        "publishedBy": triggered_by.strip() or "anonymous",
        "backupDir": _relative_to_project(backup_dir),
        "activeParquetPath": _relative_to_project(active_paths["parquet"]),
        "activeManifestPath": _relative_to_project(active_paths["manifest"]),
        "activePartitionPath": _relative_to_project(active_paths["partition"]),
        "activeFingerprintPath": _relative_to_project(active_paths["fingerprint"]),
        "activeRefreshReportPath": _relative_to_project(active_paths["refreshReport"]),
        "cacheInvalidation": cache_invalidation,
    }
    _persist_job_state(payload)
    try:
        _write_jato_publish_cache_invalidation_evidence(
            job_id=job_id,
            published_at=published_at,
            triggered_by=triggered_by,
            cache_invalidation=cache_invalidation,
            active_paths=active_paths,
        )
    except Exception as exc:
        _append_log(
            _job_log_path(job_id),
            f"[{_utc_now().isoformat()}] Hermes evidence write failed after publish: {exc}",
        )
    _append_log(
        _job_log_path(job_id),
        (
            f"[{published_at.isoformat()}] published candidate to active dataset by "
            f"{triggered_by.strip() or 'anonymous'}"
        ),
    )
    return _serialize_job_state(payload, include_log_tail=True)


def rollback_jato_monthly_update_job(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    _require_no_running_monthly_update_jobs(excluding_job_id=job_id)
    payload = _load_job_state(job_id)
    publication = payload.get("publication")
    if not isinstance(publication, dict) or not publication.get("publishedAt"):
        raise HTTPException(status_code=409, detail="当前任务还没有 publish，不能回滚。")
    if publication.get("rolledBackAt"):
        raise HTTPException(status_code=409, detail="当前任务已经执行过回滚。")

    active_paths = _active_data_paths()
    backup_dir = _project_path(str(publication.get("backupDir") or "").strip())
    if backup_dir is None or not backup_dir.exists():
        raise HTTPException(status_code=409, detail="找不到 publish 备份目录，不能回滚。")

    restore_sources = {
        "parquet": backup_dir / active_paths["parquet"].name,
        "manifest": backup_dir / active_paths["manifest"].name,
        "partition": backup_dir / active_paths["partition"].name,
        "fingerprint": backup_dir / active_paths["fingerprint"].name,
        "refreshReport": backup_dir / active_paths["refreshReport"].name,
    }
    missing_sources = [
        key
        for key, path in restore_sources.items()
        if not path.exists()
    ]
    if missing_sources:
        raise HTTPException(
            status_code=409,
            detail=f"publish 备份不完整，缺少：{', '.join(missing_sources)}。",
        )

    rolled_back_at = _utc_now()
    rollback_backup_dir = active_paths["backupRoot"] / (
        f"restore-pre-{job_id}-{rolled_back_at.strftime('%Y%m%d-%H%M%S')}"
    )
    rollback_backup_dir.mkdir(parents=True, exist_ok=True)
    if active_paths["parquet"].exists():
        shutil.copy2(active_paths["parquet"], rollback_backup_dir / active_paths["parquet"].name)
    if active_paths["manifest"].exists():
        shutil.copy2(active_paths["manifest"], rollback_backup_dir / active_paths["manifest"].name)
    if active_paths["fingerprint"].exists():
        shutil.copy2(active_paths["fingerprint"], rollback_backup_dir / active_paths["fingerprint"].name)
    if active_paths["refreshReport"].exists():
        shutil.copy2(active_paths["refreshReport"], rollback_backup_dir / active_paths["refreshReport"].name)
    if active_paths["partition"].exists():
        shutil.copytree(
            active_paths["partition"],
            rollback_backup_dir / active_paths["partition"].name,
        )

    shutil.copy2(restore_sources["parquet"], active_paths["parquet"])
    shutil.copy2(restore_sources["manifest"], active_paths["manifest"])
    shutil.copy2(restore_sources["fingerprint"], active_paths["fingerprint"])
    shutil.copy2(restore_sources["refreshReport"], active_paths["refreshReport"])
    if active_paths["partition"].exists():
        shutil.rmtree(active_paths["partition"])
    shutil.copytree(restore_sources["partition"], active_paths["partition"])

    publication["rolledBackAt"] = rolled_back_at.isoformat()
    publication["rolledBackBy"] = triggered_by.strip() or "anonymous"
    publication["rollbackBackupDir"] = _relative_to_project(rollback_backup_dir)
    payload["publication"] = publication
    _persist_job_state(payload)
    _append_log(
        _job_log_path(job_id),
        (
            f"[{rolled_back_at.isoformat()}] restored active dataset from publish backup by "
            f"{triggered_by.strip() or 'anonymous'}"
        ),
    )
    return _serialize_job_state(payload, include_log_tail=True)


def _run_logged_command(
    *,
    label: str,
    args: list[str],
    log_path: Path,
    job_id: str | None = None,
) -> None:
    tracking_job_id = job_id or _infer_job_id_from_log_path(log_path)
    if tracking_job_id:
        _ensure_job_not_cancelled(tracking_job_id)
    rendered_command = " ".join(shlex.quote(arg) for arg in args)
    _append_log(log_path, f"\n=== {label} ===\n$ {rendered_command}")
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        args,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        start_new_session=True,
    )
    if tracking_job_id:
        _set_current_process(
            job_id=tracking_job_id,
            pid=process.pid,
            label=label,
            command=rendered_command,
        )
    last_heartbeat = time.monotonic()
    try:
        if process.stdout is not None:
            for line in process.stdout:
                _append_log(log_path, line.rstrip("\n"))
                if tracking_job_id and (time.monotonic() - last_heartbeat) >= 5:
                    _touch_current_process_heartbeat(job_id=tracking_job_id, pid=process.pid)
                    last_heartbeat = time.monotonic()
    finally:
        if process.stdout is not None:
            process.stdout.close()
    return_code = process.wait()
    if tracking_job_id:
        _clear_current_process(job_id=tracking_job_id, pid=process.pid)
    if return_code != 0:
        if tracking_job_id:
            _ensure_job_not_cancelled(tracking_job_id)
        raise RuntimeError(f"{label} 失败，退出码 {return_code}")
    if tracking_job_id:
        _ensure_job_not_cancelled(tracking_job_id)


def _prepare_initial_job_state(
    *,
    job_id: str,
    month: str,
    batch_id: str | None = None,
    triggered_by: str,
    upload_filename: str,
    stored_upload_path: Path,
    file_sha256: str | None = None,
    baseline_path: Path | None = None,
    baseline_source: str | None = None,
) -> dict[str, Any]:
    now = _utc_now().isoformat()
    artifacts: dict[str, Any] = {
        "jobDir": _relative_to_project(_job_dir(job_id)),
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }
    if baseline_path is not None:
        artifacts["baselinePath"] = _relative_to_project(baseline_path)
    if baseline_source:
        artifacts["baselineSource"] = baseline_source
    return {
        "jobId": job_id,
        "month": month,
        "batchId": str(batch_id or month),
        "status": "queued",
        "phase": "queued",
        "triggeredBy": triggered_by,
        "createdAt": now,
        "updatedAt": now,
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "upload": {
            "originalFilename": upload_filename,
            "storedPath": _relative_to_project(stored_upload_path),
            "sizeBytes": stored_upload_path.stat().st_size,
            "sha256": file_sha256,
        },
        "plan": None,
        "artifacts": artifacts,
        "summaries": {},
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }


def _prepare_upload_session_state(
    *,
    upload_id: str,
    filename: str,
    size_bytes: int,
    resume_key: str | None,
    triggered_by: str,
) -> dict[str, Any]:
    now = _utc_now().isoformat()
    total_chunks = max((size_bytes + UPLOAD_CHUNK_SIZE_BYTES - 1) // UPLOAD_CHUNK_SIZE_BYTES, 1)
    return {
        "uploadId": upload_id,
        "filename": filename,
        "sizeBytes": size_bytes,
        "chunkSize": UPLOAD_CHUNK_SIZE_BYTES,
        "totalChunks": total_chunks,
        "receivedChunks": [],
        "chunkDigests": {},
        "uploadedBytes": 0,
        "status": "pending",
        "resumeKey": resume_key,
        "fileSha256": None,
        "triggeredBy": triggered_by,
        "createdAt": now,
        "updatedAt": now,
        "completedAt": None,
        "assembledPath": None,
    }


def initiate_jato_monthly_update_upload(
    *,
    filename: str,
    size_bytes: Any,
    resume_key: str | None,
    triggered_by: str,
) -> dict[str, Any]:
    normalized_filename = _validate_upload_filename(filename)
    normalized_size = _normalize_size_bytes(size_bytes)
    normalized_resume_key = str(resume_key or "").strip() or None
    existing = _find_upload_session_by_resume_key(
        resume_key=normalized_resume_key or "",
        filename=normalized_filename,
        size_bytes=normalized_size,
    )
    if existing is not None:
        return _serialize_upload_session(existing)
    upload_id = f"jato-upload-{uuid4().hex[:10]}"
    session_dir = _upload_session_dir(upload_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    _upload_session_chunk_dir(upload_id).mkdir(parents=True, exist_ok=True)
    state = _prepare_upload_session_state(
        upload_id=upload_id,
        filename=normalized_filename,
        size_bytes=normalized_size,
        resume_key=normalized_resume_key,
        triggered_by=triggered_by.strip() or "anonymous",
    )
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def get_jato_monthly_update_upload(upload_id: str) -> dict[str, Any]:
    return _serialize_upload_session(_load_upload_session(upload_id))


def upload_jato_monthly_update_chunk(
    *,
    upload_id: str,
    part_number: int,
    content: bytes,
    chunk_sha256: str,
) -> dict[str, Any]:
    state = _load_upload_session(upload_id)
    status = str(state.get("status", "pending"))
    if status in {"completed", "consumed"}:
        raise HTTPException(status_code=409, detail="上传会话已完成，不能继续写入分片。")

    total_chunks = int(state.get("totalChunks", 0))
    size_bytes = int(state.get("sizeBytes", 0))
    chunk_size = int(state.get("chunkSize", UPLOAD_CHUNK_SIZE_BYTES))
    if part_number < 1 or part_number > total_chunks:
        raise HTTPException(status_code=400, detail="分片序号超出范围。")

    expected_size = _expected_chunk_size(
        size_bytes=size_bytes,
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        part_number=part_number,
    )
    if len(content) != expected_size:
        raise HTTPException(
            status_code=400,
            detail=f"分片大小不匹配，期望 {expected_size} 字节，实际 {len(content)} 字节。",
        )
    normalized_chunk_sha256 = _normalize_sha256(
        chunk_sha256,
        detail="分片 SHA-256 无效。",
    )
    actual_chunk_sha256 = _sha256_hex_for_bytes(content)
    if actual_chunk_sha256 != normalized_chunk_sha256:
        raise HTTPException(status_code=400, detail="分片内容校验失败，请重试。")

    chunk_dir = _upload_session_chunk_dir(upload_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = chunk_dir / _chunk_file_name(part_number)
    existing_digests = state.get("chunkDigests")
    digest_map = existing_digests if isinstance(existing_digests, dict) else {}
    if (
        chunk_path.exists()
        and chunk_path.stat().st_size == expected_size
        and str(digest_map.get(str(part_number), "")) == normalized_chunk_sha256
    ):
        state["receivedChunks"] = _collect_uploaded_chunk_numbers(upload_id)
        state["uploadedBytes"] = _uploaded_chunk_bytes(upload_id)
        state["chunkDigests"] = digest_map
        state["status"] = "uploading"
        _persist_upload_session(state)
        return _serialize_upload_session(state)

    chunk_path.write_bytes(content)
    digest_map[str(part_number)] = normalized_chunk_sha256
    state["chunkDigests"] = digest_map
    state["receivedChunks"] = _collect_uploaded_chunk_numbers(upload_id)
    state["uploadedBytes"] = _uploaded_chunk_bytes(upload_id)
    state["status"] = "uploading"
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def complete_jato_monthly_update_upload(*, upload_id: str) -> dict[str, Any]:
    state = _load_upload_session(upload_id)
    total_chunks = int(state.get("totalChunks", 0))
    received_chunks = _collect_uploaded_chunk_numbers(upload_id)
    if len(received_chunks) != total_chunks or received_chunks != list(range(1, total_chunks + 1)):
        raise HTTPException(status_code=409, detail="上传分片尚未齐全，不能完成组装。")

    filename = str(state.get("filename", "jato-update.xlsx"))
    assembled_path = _upload_session_assembled_path(upload_id, filename)
    assembled_path.parent.mkdir(parents=True, exist_ok=True)
    existing_digests = state.get("chunkDigests")
    digest_map = existing_digests if isinstance(existing_digests, dict) else {}
    file_hasher = hashlib.sha256()
    with assembled_path.open("wb") as target:
        for part_number in range(1, total_chunks + 1):
            chunk_path = _upload_session_chunk_dir(upload_id) / _chunk_file_name(part_number)
            expected_digest = str(digest_map.get(str(part_number), "")).strip().lower()
            with chunk_path.open("rb") as source:
                chunk_bytes = source.read()
            if not chunk_bytes:
                raise HTTPException(status_code=500, detail="发现空分片，无法完成组装。")
            if expected_digest:
                actual_chunk_digest = _sha256_hex_for_bytes(chunk_bytes)
                if actual_chunk_digest != expected_digest:
                    raise HTTPException(status_code=500, detail="分片校验失败，请重新上传缺失分片。")
            target.write(chunk_bytes)
            file_hasher.update(chunk_bytes)

    if assembled_path.stat().st_size != int(state.get("sizeBytes", 0)):
        raise HTTPException(status_code=500, detail="组装后的文件大小校验失败，请重新上传。")

    shutil.rmtree(_upload_session_chunk_dir(upload_id), ignore_errors=True)
    state["receivedChunks"] = received_chunks
    state["uploadedBytes"] = assembled_path.stat().st_size
    state["status"] = "completed"
    state["completedAt"] = _utc_now().isoformat()
    state["assembledPath"] = _relative_to_project(assembled_path)
    state["fileSha256"] = file_hasher.hexdigest()
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def _parse_month_from_filename(filename: str) -> str | None:
    """Try to extract YYYY-MM from filename. Returns None if not found."""
    import re

    name = Path(filename).stem
    patterns = [
        r"(20\d{2})[._-](\d{1,2})",     # 2026.4, 2026-04, 2026_3
        r"(20\d{2})\s*[年-]\s*(\d{1,2})",  # 2026年4, 2026-3
    ]
    for pat in patterns:
        m = re.search(pat, name)
        if m:
            year = int(m.group(1))
            month_num = int(m.group(2))
            if 1 <= month_num <= 12 and 2020 <= year <= 2030:
                return f"{year}-{month_num:02d}"
    return None


def _queue_monthly_update_job_from_stored_upload(
    *,
    job_id: str,
    triggered_by: str,
    upload_filename: str,
    stored_upload_path: Path,
    month: str,
    file_sha256: str | None = None,
) -> dict[str, Any]:
    if stored_upload_path.stat().st_size <= 0:
        raise HTTPException(status_code=400, detail="上传文件为空，无法启动月更任务。")

    normalized_month = _normalize_month(month)
    batch_id = _allocate_batch_id(normalized_month)

    now = _utc_now().isoformat()
    state: dict[str, Any] = {
        "jobId": job_id,
        "month": normalized_month,
        "batchId": batch_id,
        "status": "queued",
        "phase": "queued",
        "triggeredBy": triggered_by.strip() or "anonymous",
        "createdAt": now,
        "updatedAt": now,
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "upload": {
            "originalFilename": upload_filename,
            "storedPath": _relative_to_project(stored_upload_path),
            "sizeBytes": stored_upload_path.stat().st_size,
            "sha256": file_sha256,
        },
        "plan": None,
        "artifacts": {
            "jobDir": _relative_to_project(_job_dir(job_id)),
            "logPath": _relative_to_project(_job_log_path(job_id)),
        },
        "summaries": {},
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }
    _persist_job_state(state)
    _append_log(
        _job_log_path(job_id),
        f"[{now}] queued monthly update batch {batch_id} for month {normalized_month}",
    )
    _launch_job_thread(job_id)
    return _serialize_job_state(state, include_log_tail=False)

# Large file guard for direct multipart upload (not chunked)
_MAX_DIRECT_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB


def _run_job(job_id: str) -> None:
    state = _load_job_state(job_id)
    if str(state.get("status") or "") == "cancelled":
        _RUNNING_THREADS.pop(job_id, None)
        return
    log_path = _job_log_path(job_id)
    upload_payload = state.get("upload")
    if not isinstance(upload_payload, dict):
        raise RuntimeError("任务缺少 upload 信息")
    stored_path_value = upload_payload.get("storedPath")
    if not stored_path_value:
        raise RuntimeError("任务缺少 upload storedPath")
    stored_upload_path = PROJECT_ROOT / str(stored_path_value)

    state["status"] = "running"
    state["startedAt"] = _utc_now().isoformat()

    try:
        partial_inspection = _detect_partial_country_upload(stored_upload_path)
    except Exception as exc:
        state["status"] = "failed"
        state["phase"] = "failed"
        state["finishedAt"] = _utc_now().isoformat()
        state["error"] = f"上传范围识别失败：{exc}"
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, f"[{_utc_now().isoformat()}] {state['error']}")
        _RUNNING_THREADS.pop(job_id, None)
        return

    if partial_inspection is not None:
        countries = [
            str(country).strip()
            for country in partial_inspection["countries"]
            if str(country).strip()
        ]
        detected_month = str(partial_inspection["latestMonth"])
        is_single_country = len(countries) == 1
        state["jobType"] = "single_country" if is_single_country else "partial_country"
        state["country"] = countries[0] if is_single_country else None
        state["countryScope"] = countries
        state["month"] = detected_month
        state["batchId"] = (
            f"{detected_month}-{countries[0]}-single"
            if is_single_country
            else f"{detected_month}-partial-{len(countries)}c"
        )
        state["uploadInspection"] = partial_inspection
        _persist_job_state(state)
        _append_log(
            log_path,
            (
                f"[{_utc_now().isoformat()}] 自动识别部分国家上传："
                f"countries={','.join(countries)}, month={detected_month}；"
                "跳过全量 baseline Raw Compare，转入目标分区 Review 路径。"
            ),
        )
        _run_country_partition_job(job_id)
        return

    batch_id = str(state.get("batchId") or "")
    if not batch_id:
        raise RuntimeError("任务缺少批次标识")

    try:
        state["phase"] = "preparing"
        _persist_job_state(state)
        artifacts = state.get("artifacts")
        baseline_relative_path = (
            artifacts.get("baselinePath")
            if isinstance(artifacts, dict)
            else None
        )
        baseline_source = (
            str(artifacts.get("baselineSource"))
            if isinstance(artifacts, dict) and artifacts.get("baselineSource")
            else None
        )
        baseline_path = _project_path(str(baseline_relative_path)) if baseline_relative_path else None
        if baseline_path is None:
            baseline_path, baseline_source = _require_latest_baseline()
            state["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
            state["artifacts"]["baselinePath"] = _relative_to_project(baseline_path)
            state["artifacts"]["baselineSource"] = baseline_source
            _persist_job_state(state)
        elif not baseline_path.exists():
            raise RuntimeError(
                "任务绑定的 baseline 不存在："
                f"{_relative_to_project(baseline_path) or str(baseline_path)}"
            )

        if baseline_source == "archive":
            _append_log(
                log_path,
                (
                    f"[{_utc_now().isoformat()}] active baseline 缺失，"
                    "使用 archive baseline: "
                    f"{_relative_to_project(baseline_path) or str(baseline_path)}"
                ),
            )

        if not PREPARE_SCRIPT_PATH.exists():
            raise RuntimeError(f"找不到脚本: {_relative_to_project(PREPARE_SCRIPT_PATH)}")

        _ensure_job_not_cancelled(job_id)
        prepare_args = [
            sys.executable,
            str(PREPARE_SCRIPT_PATH),
            "--month",
            str(state["month"]),
            "--batch-id",
            batch_id,
            "--baseline",
            str(baseline_path),
            "--patch",
            str(stored_upload_path),
        ]
        _run_logged_command(
            label="Prepare monthly update",
            args=prepare_args,
            log_path=log_path,
        )

        plan_path = PROJECT_ROOT / "01_RAW_DATA" / "patches" / batch_id / "monthly_update_plan.md"
        if not plan_path.exists():
            raise RuntimeError("prepare 完成后未生成 monthly_update_plan.md")

        parsed_plan = _parse_plan_markdown(plan_path)
        refresh_command, supplement_parquet_path = _inject_refresh_supplement_arg(
            str(parsed_plan["refreshCommand"])
        )
        parsed_plan["refreshCommand"] = refresh_command
        if supplement_parquet_path:
            parsed_plan["supplementParquetPath"] = supplement_parquet_path
        state["plan"] = {
            "path": _relative_to_project(plan_path),
            "batchId": parsed_plan.get("batchId") or batch_id,
            "compareId": parsed_plan.get("compareId"),
            "compareCommand": parsed_plan.get("compareCommand"),
            "refreshCommand": parsed_plan.get("refreshCommand"),
        }
        artifacts = state.get("artifacts")
        state["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
        state["artifacts"].update(
            {
                "jobDir": _relative_to_project(_job_dir(job_id)),
                "logPath": _relative_to_project(log_path),
                "baselinePath": parsed_plan.get("baselinePath"),
                "stagedPatchPath": parsed_plan.get("patchPath"),
                "supplementParquetPath": parsed_plan.get("supplementParquetPath"),
                "planPath": _relative_to_project(plan_path),
                "reviewDir": parsed_plan.get("reviewDir"),
                "rawCompareReportPath": parsed_plan.get("rawCompareReportPath"),
                "stagingOutputPath": parsed_plan.get("stagingOutputPath"),
                "manifestPath": parsed_plan.get("manifestPath"),
                "partitionOutputPath": parsed_plan.get("partitionOutputPath"),
                "refreshReportPath": parsed_plan.get("refreshReportPath"),
                "fingerprintPath": parsed_plan.get("fingerprintPath"),
            }
        )
        _persist_job_state(state)

        state["phase"] = "raw_compare"
        _persist_job_state(state)
        _ensure_job_not_cancelled(job_id)
        compare_args = _command_to_args(str(parsed_plan["compareCommand"]))
        _run_logged_command(
            label="Raw compare review",
            args=compare_args,
            log_path=log_path,
        )
        raw_compare_report = _read_json_if_exists(parsed_plan.get("rawCompareReportPath"))
        if raw_compare_report is None:
            raise RuntimeError("raw compare 完成后未找到 raw_compare_report.json")
        summaries = state.get("summaries")
        state["summaries"] = summaries if isinstance(summaries, dict) else {}
        state["summaries"]["rawCompare"] = _summarize_raw_compare_report(raw_compare_report)
        _persist_job_state(state)

        state["phase"] = "refresh"
        _persist_job_state(state)
        _ensure_job_not_cancelled(job_id)
        refresh_args = _command_to_args(str(parsed_plan["refreshCommand"]))
        _run_logged_command(
            label="Candidate refresh",
            args=refresh_args,
            log_path=log_path,
        )
        refresh_report = _read_json_if_exists(parsed_plan.get("refreshReportPath"))
        if refresh_report is None:
            raise RuntimeError("refresh 完成后未找到 refresh_job_report.json")
        state["summaries"]["refresh"] = _summarize_refresh_report(refresh_report)
        state["status"] = "success"
        state["phase"] = "completed"
        state["finishedAt"] = _utc_now().isoformat()
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
    except _JobCancelled as exc:
        state = _load_job_state(job_id)
        state["status"] = "cancelled"
        state["phase"] = "cancelled"
        state["finishedAt"] = state.get("finishedAt") or _utc_now().isoformat()
        state["error"] = str(exc)
        state["currentProcess"] = None
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, f"[{_utc_now().isoformat()}] Cancelled: {exc}")
    except Exception as exc:
        state["status"] = "failed"
        state["phase"] = "failed"
        state["finishedAt"] = _utc_now().isoformat()
        state["error"] = str(exc)
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, "\n=== Failed ===")
        _append_log(log_path, str(exc))
        _append_log(log_path, traceback.format_exc())
    finally:
        _RUNNING_THREADS.pop(job_id, None)


def _launch_job_thread(job_id: str) -> None:
    worker = threading.Thread(
        target=_run_job,
        args=(job_id,),
        name=f"jato-monthly-update-{job_id}",
        daemon=True,
    )
    _RUNNING_THREADS[job_id] = worker
    worker.start()


def _active_partition_country_names(partition_root: Path | None = None) -> list[str]:
    root = partition_root or _active_data_paths()["partition"]
    if not root.exists():
        return []
    prefix = "国家="
    return sorted(
        {
            unquote(path.name[len(prefix):])
            for path in root.iterdir()
            if path.is_dir() and path.name.startswith(prefix)
        }
    )


def _normalized_month_from_label(label: str) -> str:
    parsed = datetime.strptime(str(label).strip().title(), "%Y %b")
    return f"{parsed.year}-{parsed.month:02d}"


def _inspect_upload_scope(path: Path) -> dict[str, Any]:
    """Inspect country/month scope without materializing every Excel column."""
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        try:
            from openpyxl import load_workbook

            workbook = load_workbook(
                path,
                read_only=True,
                data_only=True,
                keep_links=False,
            )
            try:
                if DEFAULT_UPLOAD_SHEET_NAME not in workbook.sheetnames:
                    raise RuntimeError(
                        f"上传 Excel 缺少工作表：{DEFAULT_UPLOAD_SHEET_NAME}。"
                    )
                worksheet = workbook[DEFAULT_UPLOAD_SHEET_NAME]
                rows = worksheet.iter_rows(values_only=True)
                header_row = next(rows, None)
                if header_row is None:
                    raise RuntimeError("上传 Excel 的 Data Export 为空。")
                columns = [str(value).strip() if value is not None else "" for value in header_row]
                country_column = _find_country_column(columns)
                if country_column is None:
                    raise RuntimeError("上传 Excel 的 Data Export 缺少国家列。")
                country_index = columns.index(country_column)
                month_columns = _detect_month_columns(columns)
                if not month_columns:
                    raise RuntimeError("上传 Excel 的 Data Export 缺少可识别月份列。")
                month_indexes = [
                    (columns.index(month_column), month_column)
                    for month_column in month_columns
                ]
                countries: list[str] = []
                seen_countries: set[str] = set()
                country_latest_labels: dict[str, str | None] = {}
                data_row_count = 0
                for row in rows:
                    raw_country = row[country_index] if country_index < len(row) else None
                    country = str(raw_country).strip() if raw_country is not None else ""
                    if not country:
                        continue
                    data_row_count += 1
                    if country not in seen_countries:
                        seen_countries.add(country)
                        countries.append(country)
                        country_latest_labels[country] = None
                    for month_index, month_label in reversed(month_indexes):
                        value = row[month_index] if month_index < len(row) else None
                        if value is None or (isinstance(value, str) and not value.strip()):
                            continue
                        current = country_latest_labels[country]
                        if current is None or _time_sort_key(month_label) > _time_sort_key(current):
                            country_latest_labels[country] = month_label
                        break
            finally:
                workbook.close()
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"无法轻量读取上传 Excel：{exc}") from exc
    else:
        frame = _read_excel_with_fallback(path, sheet_name=DEFAULT_UPLOAD_SHEET_NAME)
        frame.columns = [str(column).strip() for column in frame.columns]
        country_column = _find_country_column(list(frame.columns))
        if country_column is None:
            raise RuntimeError("上传 Excel 的 Data Export 缺少国家列。")
        month_columns = _detect_month_columns(list(frame.columns))
        if not month_columns:
            raise RuntimeError("上传 Excel 的 Data Export 缺少可识别月份列。")
        frame[country_column] = frame[country_column].astype("string").fillna("").str.strip()
        countries = _ordered_distinct_strings(frame[country_column].tolist())
        country_latest_labels = {}
        for country in countries:
            country_frame = frame.loc[frame[country_column] == country]
            country_latest_labels[country] = _latest_month_from_frame(country_frame)
        data_row_count = int((frame[country_column] != "").sum())

    if not countries:
        raise RuntimeError("上传 Excel 的 Data Export 不包含有效国家。")
    latest_labels = [
        label for label in country_latest_labels.values()
        if label is not None
    ]
    if not latest_labels:
        raise RuntimeError("上传 Excel 的月份列均为空。")
    latest_label = max(latest_labels, key=_time_sort_key)
    return {
        "sheetName": DEFAULT_UPLOAD_SHEET_NAME,
        "countries": countries,
        "countryLatestMonths": {
            country: (
                _normalized_month_from_label(label)
                if label is not None
                else None
            )
            for country, label in country_latest_labels.items()
        },
        "latestMonth": _normalized_month_from_label(latest_label),
        "dataRowCount": data_row_count,
    }


def _detect_partial_country_upload(path: Path) -> dict[str, Any] | None:
    inspection = _inspect_upload_scope(path)
    active_countries = _active_partition_country_names()
    uploaded_countries = set(inspection["countries"])
    if (
        active_countries
        and uploaded_countries.issubset(set(active_countries))
        and len(uploaded_countries) < len(active_countries)
    ):
        return inspection
    return None


def _detect_single_country_upload(path: Path) -> tuple[str, str] | None:
    """Return (country, month) for a one-country Data Export."""
    try:
        inspection = _inspect_upload_scope(path)
        countries = inspection["countries"]
        if len(countries) != 1:
            return None
        return countries[0], str(inspection["latestMonth"])
    except Exception:
        return None


def _queue_single_country_job(
    *,
    job_id: str,
    country: str,
    month: str,
    triggered_by: str,
    upload_filename: str,
    stored_upload_path: Path,
) -> dict[str, Any]:
    """Create job state and launch single-country background runner."""
    normalized_month = _normalize_month(month)
    normalized_country = country.strip()

    active_paths = _active_data_paths()
    if active_paths["parquet"].exists():
        try:
            active_frame = (
                _load_active_country_partition_subset(active_paths["partition"], normalized_country)
                if active_paths["partition"].exists()
                else _load_parquet_country_subset(
                    active_paths["parquet"], normalized_country, path_label="active"
                )
            )
            active_latest = _latest_month_from_frame(active_frame)
            if active_latest and normalized_month <= active_latest:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"国家「{normalized_country}」在 active 数据集中已有 {active_latest} 月的数据，"
                        f"上传月份 {normalized_month} 不更新。请提供晚于 {active_latest} 的月份。"
                    ),
                )
        except HTTPException:
            raise
        except Exception:
            pass

    job_dir = _job_dir(job_id)
    now = _utc_now().isoformat()
    batch_id = f"{normalized_month}-{normalized_country}-single"
    state: dict[str, Any] = {
        "jobId": job_id,
        "month": normalized_month,
        "batchId": batch_id,
        "status": "queued",
        "phase": "queued",
        "jobType": "single_country",
        "triggeredBy": triggered_by.strip() or "anonymous",
        "country": normalized_country,
        "countryScope": [normalized_country],
        "createdAt": now,
        "updatedAt": now,
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "upload": {
            "originalFilename": upload_filename,
            "storedPath": _relative_to_project(stored_upload_path),
            "sizeBytes": stored_upload_path.stat().st_size,
            "sha256": _sha256_hex_for_path(stored_upload_path),
        },
        "plan": None,
        "artifacts": {
            "jobDir": _relative_to_project(job_dir),
            "logPath": _relative_to_project(_job_log_path(job_id)),
        },
        "summaries": {},
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }
    _persist_job_state(state)
    _append_log(
        _job_log_path(job_id),
        f"[{now}] 已入队单国家任务：{normalized_country} {normalized_month}",
    )

    worker = threading.Thread(
        target=_run_single_country_job,
        args=(job_id,),
        name=f"jato-sc-{job_id}",
        daemon=True,
    )
    _RUNNING_THREADS[job_id] = worker
    worker.start()

    return _serialize_job_state(state, include_log_tail=False)


def create_jato_monthly_update_job(
    *,
    file: UploadFile,
    triggered_by: str,
) -> dict[str, Any]:
    filename = _validate_upload(file)

    file_size = file.size or 0
    if file_size > _MAX_DIRECT_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"文件 {file_size / 1024 / 1024:.0f}MB 超过 {_MAX_DIRECT_UPLOAD_BYTES / 1024 / 1024:.0f}MB，"
                "请使用分片上传（chunked upload）。"
            ),
        )

    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename

    with stored_upload_path.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    month = (
        _parse_month_from_filename(filename)
        or datetime.now().strftime("%Y-%m")
    )
    return _queue_monthly_update_job_from_stored_upload(
        job_id=job_id,
        triggered_by=triggered_by,
        upload_filename=filename,
        stored_upload_path=stored_upload_path,
        month=month,
    )


def create_jato_monthly_update_job_from_upload(
    *,
    upload_id: str,
    triggered_by: str,
    month: str | None = None,
) -> dict[str, Any]:
    state = _load_upload_session(upload_id)
    if str(state.get("status", "")) != "completed":
        raise HTTPException(status_code=409, detail="上传尚未完成组装，不能创建月更任务。")

    filename = _validate_upload_filename(str(state.get("filename", "jato-update.xlsx")))
    assembled_path = _upload_session_assembled_path(upload_id, filename)
    if not assembled_path.exists():
        raise HTTPException(status_code=409, detail="组装后的上传文件不存在，请重新上传。")
    file_sha256 = _normalize_sha256(
        state.get("fileSha256"),
        detail="上传文件指纹缺失，请重新完成组装。",
    )

    resolved_month = (
        month
        or _parse_month_from_filename(filename)
    )
    if not resolved_month:
        raise HTTPException(
            status_code=400,
            detail="无法从文件名解析月份，请在上传时明确指定 month 参数。",
        )

    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename
    shutil.move(str(assembled_path), str(stored_upload_path))
    try:
        result = _queue_monthly_update_job_from_stored_upload(
            job_id=job_id,
            triggered_by=triggered_by,
            upload_filename=filename,
            stored_upload_path=stored_upload_path,
            month=resolved_month,
            file_sha256=file_sha256,
        )
    except Exception:
        upload_session_dir = _upload_session_dir(upload_id)
        upload_session_dir.mkdir(parents=True, exist_ok=True)
        if stored_upload_path.exists() and not assembled_path.exists():
            shutil.move(str(stored_upload_path), str(assembled_path))
        shutil.rmtree(_job_dir(job_id), ignore_errors=True)
        raise
    shutil.rmtree(_upload_session_dir(upload_id), ignore_errors=True)
    return result


def retry_failed_jato_monthly_update_job(
    *,
    source_job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    source_state = _load_job_state(source_job_id)
    if str(source_state.get("status", "")) != "failed":
        raise HTTPException(
            status_code=409,
            detail="只有 failed 的月更任务才能直接重试。",
        )

    source_upload = source_state.get("upload")
    if not isinstance(source_upload, dict):
        raise HTTPException(status_code=409, detail="原任务缺少 upload 信息，无法直接重试。")

    stored_path_value = source_upload.get("storedPath")
    if not stored_path_value:
        raise HTTPException(
            status_code=409,
            detail="原任务的上传副本已不存在，请重新上传文件后再试。",
        )

    source_upload_path = _project_path(str(stored_path_value))
    if source_upload_path is None or not source_upload_path.exists():
        raise HTTPException(
            status_code=409,
            detail="原任务的上传副本已不存在，请重新上传文件后再试。",
        )

    filename = _validate_upload_filename(
        str(source_upload.get("originalFilename") or source_upload_path.name)
    )
    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename
    shutil.copy2(source_upload_path, stored_upload_path)
    source_month = str(source_state.get("month") or "").strip()
    retry_month = (
        source_month
        or _parse_month_from_filename(filename)
        or datetime.now().strftime("%Y-%m")
    )
    try:
        result = _queue_monthly_update_job_from_stored_upload(
            job_id=job_id,
            triggered_by=triggered_by,
            upload_filename=filename,
            stored_upload_path=stored_upload_path,
            month=retry_month,
            file_sha256=(
                str(source_upload.get("sha256"))
                if source_upload.get("sha256") is not None
                else None
            ),
        )
    except Exception:
        shutil.rmtree(_job_dir(job_id), ignore_errors=True)
        raise

    payload = _load_job_state(job_id)
    artifacts = payload.get("artifacts")
    payload["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
    payload["artifacts"]["retriedFromJobId"] = source_job_id
    _persist_job_state(payload)
    return _serialize_job_state(payload, include_log_tail=True)


# ── Country-Partition Upload ───────────────────────────────────────────────────


def _run_country_partition_job(job_id: str) -> None:
    """Build a Review-only candidate for a strict subset of active countries."""
    state = _load_job_state(job_id)
    if str(state.get("status") or "") == "cancelled":
        _RUNNING_THREADS.pop(job_id, None)
        return
    log_path = _job_log_path(job_id)

    try:
        state["status"] = "running"
        state["phase"] = "validating"
        _persist_job_state(state)

        upload = state.get("upload") or {}
        stored_path_value = upload.get("storedPath") if isinstance(upload, dict) else None
        stored_upload_path = _project_path(str(stored_path_value or "").strip())
        if stored_upload_path is None or not stored_upload_path.exists():
            raise RuntimeError("上传文件不存在。")

        countries = _ordered_distinct_strings(
            [
                str(country)
                for country in (
                    state.get("countryScope")
                    if isinstance(state.get("countryScope"), list)
                    else [state.get("country")]
                )
                if country is not None
            ]
        )
        month = str(state.get("month", "")).strip()
        if not countries or not month:
            raise RuntimeError("任务状态缺少 countryScope 或 month。")

        suffix = stored_upload_path.suffix.lower()
        if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
            raise RuntimeError(f"不支持的文件格式：{suffix}，仅支持 Excel。")

        inspection = state.get("uploadInspection")
        if not isinstance(inspection, dict):
            inspection = _inspect_upload_scope(stored_upload_path)
        uploaded_countries = _ordered_distinct_strings(
            [str(country) for country in inspection.get("countries", [])]
        )
        if uploaded_countries != countries:
            raise RuntimeError(
                "上传文件的国家范围与任务绑定范围不一致："
                f"expected={','.join(countries)}，actual={','.join(uploaded_countries) or '-'}"
            )

        _append_log(
            log_path,
            (
                f"[{_utc_now().isoformat()}] 验证通过："
                f"countries={','.join(countries)}, month={month}"
            ),
        )

        # Staging paths under job dir
        job_dir = _job_dir(job_id)
        staging_dir = job_dir / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staging_output = staging_dir / "country_partition_candidate.parquet"
        manifest_path = staging_dir / "manifest.json"
        report_path = staging_dir / "refresh_job_report.json"
        active_paths = _active_data_paths()
        if not active_paths["partition"].exists():
            raise RuntimeError("部分国家刷新需要 active partitioned dataset，当前分区目录不存在。")
        missing_active_countries = [
            country
            for country in countries
            if not (
                active_paths["partition"]
                / f"国家={quote(country, safe='')}"
            ).exists()
        ]
        if missing_active_countries:
            raise RuntimeError(
                "active 分区缺少上传国家："
                + "、".join(missing_active_countries)
            )
        untouched_before = _untouched_partition_snapshot(
            partition_root=active_paths["partition"],
            countries=countries,
        )

        state["phase"] = "refreshing"
        _persist_job_state(state)
        _ensure_job_not_cancelled(job_id)

        refresh_args = [
            sys.executable,
            str(SINGLE_COUNTRY_ETL_SCRIPT_PATH),
            "--input",
            str(stored_upload_path.resolve()),
            "--output",
            str(staging_output.resolve()),
            "--manifest",
            str(manifest_path.resolve()),
            "--job-id",
            job_id,
        ]

        _run_logged_command(
            label="部分国家目标分区转换",
            args=refresh_args,
            log_path=log_path,
        )
        country_latest_months = (
            inspection.get("countryLatestMonths")
            if isinstance(inspection.get("countryLatestMonths"), dict)
            else {}
        )
        country_results: list[dict[str, Any]] = []
        total_rows = 0
        candidate_column_count = 0
        for country in countries:
            candidate_frame = _load_parquet_country_subset(
                staging_output,
                country,
                path_label="country-partition candidate",
            )
            if candidate_frame.empty:
                raise RuntimeError(f"candidate 不包含目标国家：{country}")
            candidate_latest = _latest_month_from_frame(candidate_frame)
            expected_month = str(country_latest_months.get(country) or "").strip()
            expected_month_label = (
                datetime.strptime(expected_month, "%Y-%m").strftime("%Y %b")
                if expected_month
                else None
            )
            if candidate_latest != expected_month_label:
                raise RuntimeError(
                    f"{country} candidate 的最新月份与上传识别不一致："
                    f"expected={expected_month_label or '-'}, actual={candidate_latest or '-'}"
                )
            active_frame = _load_active_country_partition_subset(
                active_paths["partition"],
                country,
            )
            active_latest = _latest_month_from_frame(active_frame)
            if (
                active_latest
                and candidate_latest
                and _time_sort_key(candidate_latest) < _time_sort_key(active_latest)
            ):
                raise RuntimeError(
                    f"{country} 最新月份发生回退："
                    f"active={active_latest}, candidate={candidate_latest}"
                )
            total_rows += int(len(candidate_frame))
            candidate_column_count = max(
                candidate_column_count,
                int(len(candidate_frame.columns)),
            )
            country_results.append(
                {
                    "country": country,
                    "rows": int(len(candidate_frame)),
                    "activeLatestMonth": active_latest,
                    "candidateLatestMonth": candidate_latest,
                }
            )
            del active_frame
            del candidate_frame
        untouched_after = _untouched_partition_snapshot(
            partition_root=active_paths["partition"],
            countries=countries,
        )
        untouched_partition_check = _verify_untouched_partition_stability(
            before=untouched_before,
            after=untouched_after,
        )
        candidate_scope = (
            "target_country_partition_only"
            if len(countries) == 1
            else "target_country_partitions_only"
        )
        refresh_report = {
            "jobStatus": "success",
            "fullManifest": {
                "rows": total_rows,
                "columns": candidate_column_count,
            },
            "partitionManifest": {"parquetFileCount": len(countries)},
            "incremental": {
                "enabled": True,
                "scope": candidate_scope,
                "targetCountry": countries[0] if len(countries) == 1 else None,
                "targetCountries": countries,
                "countryResults": country_results,
                "untouchedPartitionCheck": untouched_partition_check,
            },
        }
        _write_json(report_path, refresh_report)

        state["artifacts"] = {
            "jobDir": _relative_to_project(job_dir),
            "logPath": _relative_to_project(log_path),
            "baselinePath": None,
            "baselineSource": "active_country_partition",
            "stagedPatchPath": _relative_to_project(stored_upload_path),
            "supplementParquetPath": None,
            "stagingOutputPath": _relative_to_project(staging_output),
            "manifestPath": _relative_to_project(manifest_path),
            "partitionOutputPath": None,
            "refreshReportPath": _relative_to_project(report_path),
            "fingerprintPath": None,
            "reviewDir": None,
            "rawCompareReportPath": None,
            "planPath": None,
            "candidateScope": candidate_scope,
            "untouchedPartitionCheck": untouched_partition_check,
        }
        state["summaries"] = {
            "refresh": _summarize_refresh_report(refresh_report),
            "jobInfo": {
                "country": countries[0] if len(countries) == 1 else None,
                "countries": countries,
                "month": month,
                "type": str(state.get("jobType") or "partial_country"),
            },
        }
        state["plan"] = None
        state["status"] = "success"
        state["phase"] = "completed"
        state["finishedAt"] = _utc_now().isoformat()
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(
            log_path,
            (
                f"[{_utc_now().isoformat()}] 部分国家任务完成："
                f"{','.join(countries)} {month}。可前往 Review（不改变 active）。"
            ),
        )

    except _JobCancelled as exc:
        state = _load_job_state(job_id)
        state["status"] = "cancelled"
        state["phase"] = "cancelled"
        state["finishedAt"] = state.get("finishedAt") or _utc_now().isoformat()
        state["error"] = str(exc)
        state["currentProcess"] = None
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, f"[{_utc_now().isoformat()}] Cancelled: {exc}")
    except Exception as exc:
        state["status"] = "failed"
        state["phase"] = "failed"
        state["finishedAt"] = _utc_now().isoformat()
        state["error"] = str(exc)
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, "\n=== Failed ===")
        _append_log(log_path, str(exc))
        _append_log(log_path, traceback.format_exc())
    finally:
        _RUNNING_THREADS.pop(job_id, None)


def _run_single_country_job(job_id: str) -> None:
    """Compatibility wrapper for the explicit single-country API."""
    _run_country_partition_job(job_id)


def create_single_country_job(
    *,
    country: str,
    month: str,
    file: UploadFile,
    triggered_by: str,
) -> dict[str, Any]:
    """Create a lightweight single-country single-month upload job (no prepare/raw_compare)."""
    _require_no_running_monthly_update_jobs()
    filename = _validate_upload(file)
    normalized_month = _normalize_month(month)
    normalized_country = country.strip()
    if not normalized_country:
        raise HTTPException(status_code=400, detail="country 不能为空。")

    job_id = f"jato-sc-{uuid4().hex[:8]}"
    job_dir = _job_dir(job_id)
    uploads_dir = job_dir / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename

    with stored_upload_path.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    return _queue_single_country_job(
        job_id=job_id,
        country=normalized_country,
        month=normalized_month,
        triggered_by=triggered_by,
        upload_filename=filename,
        stored_upload_path=stored_upload_path,
    )


# ── Smart Merge ──────────────────────────────────────────────────────────────


def _static_carry_forward_key_columns(
    *,
    active_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
) -> list[str]:
    shared = [
        column
        for column in STATIC_CARRY_FORWARD_KEY_CANDIDATES
        if column in active_frame.columns and column in candidate_frame.columns
    ]
    if "Make" not in shared or "Model" not in shared:
        return []
    if "Version name" not in shared and "Trim level" not in shared:
        return []
    return shared


def _normalized_static_value_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series.dtype):
        numbers = pd.to_numeric(series, errors="coerce")
        return numbers.map(
            lambda value: (
                ""
                if pd.isna(value)
                else format(float(value), ".15g")
            )
        )
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.casefold()
    )


def _normalized_static_key_frame(
    frame: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            f"__key_{index}": _normalized_static_value_series(frame[column])
            for index, column in enumerate(key_columns)
        },
        index=frame.index,
    )


def _carry_forward_deprecated_static_columns(
    *,
    active_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Retain deprecated values only when an active config key has one value."""
    columns = sorted(
        column
        for column in DEPRECATED_OPTIONAL_STATIC_COLUMNS
        if column in active_frame.columns
        and (
            column not in candidate_frame.columns
            or bool(_series_missing_value_mask(candidate_frame[column]).any())
        )
        and _series_has_data(active_frame[column])
    )
    summary: dict[str, Any] = {
        "enabled": bool(columns),
        "policy": "consistent_active_key_values_only",
        "columns": columns,
        "keyColumns": [],
        "candidateRowCount": int(len(candidate_frame)),
        "matchedConfigurationRowCount": 0,
        "candidateDuplicateKeyRowCount": 0,
        "activeDuplicateKeyRowCount": 0,
        "columnResults": {},
    }
    if not columns:
        return candidate_frame, summary

    key_columns = _static_carry_forward_key_columns(
        active_frame=active_frame,
        candidate_frame=candidate_frame,
    )
    summary["keyColumns"] = key_columns
    if not key_columns:
        summary["enabled"] = False
        summary["reason"] = "stable_configuration_keys_unavailable"
        return candidate_frame, summary

    active_keys = _normalized_static_key_frame(active_frame, key_columns)
    candidate_keys = _normalized_static_key_frame(candidate_frame, key_columns)
    key_names = list(active_keys.columns)
    summary["activeDuplicateKeyRowCount"] = int(
        active_keys.duplicated(keep=False).sum()
    )
    summary["candidateDuplicateKeyRowCount"] = int(
        candidate_keys.duplicated(keep=False).sum()
    )

    candidate_lookup = candidate_keys.copy()
    candidate_lookup["__candidate_index"] = candidate_lookup.index
    result = candidate_frame.copy()
    matched_candidate_indices: set[Any] = set()
    for index, column in enumerate(columns):
        if column not in result.columns:
            result[column] = pd.NA
        carry_column = f"__carry_{index}"
        value_signature_column = f"__carry_signature_{index}"
        active_values = pd.concat(
            [
                active_keys,
                active_frame[column].rename(carry_column),
                _normalized_static_value_series(active_frame[column]).rename(
                    value_signature_column
                ),
            ],
            axis=1,
        )
        active_values = active_values.loc[
            ~_series_missing_value_mask(active_values[carry_column])
        ]
        distinct_values = active_values.drop_duplicates(
            subset=[*key_names, value_signature_column]
        )
        unambiguous_values = distinct_values.loc[
            ~distinct_values.duplicated(subset=key_names, keep=False)
        ]
        matched = candidate_lookup.reset_index(drop=True).merge(
            unambiguous_values[[*key_names, carry_column]],
            how="inner",
            on=key_names,
            validate="many_to_one",
        )
        candidate_indices = matched["__candidate_index"].tolist()
        current_missing = _series_missing_value_mask(
            result.loc[candidate_indices, column]
        )
        rows_to_fill = matched.loc[current_missing.to_numpy()]
        if not rows_to_fill.empty:
            result.loc[
                rows_to_fill["__candidate_index"].tolist(),
                column,
            ] = rows_to_fill[carry_column].tolist()
        matched_candidate_indices.update(rows_to_fill["__candidate_index"].tolist())
        summary["columnResults"][column] = {
            "inheritedRowCount": int(len(rows_to_fill)),
            "ambiguousActiveKeyCount": int(
                distinct_values.loc[
                    distinct_values.duplicated(
                        subset=key_names,
                        keep=False,
                    ),
                    key_names,
                ]
                .drop_duplicates()
                .shape[0]
            ),
            "remainingNullRowCount": int(
                _series_missing_value_mask(result[column]).sum()
            ),
        }
    summary["matchedConfigurationRowCount"] = len(matched_candidate_indices)
    return result, summary


def _smart_merge_dataframes(
    *,
    active_path: Path,
    candidate_path: Path,
    regressed_countries: list[dict[str, str | None]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Merge active + candidate parquet data for Smart Merge.

    - Regressed countries: use active data (has more recent months)
    - Advanced/unchanged/new countries: use candidate data
    - Countries only in active, missing in candidate: use active data
    """
    active_df = _load_dataset_frame(active_path)
    candidate_df = _load_dataset_frame(candidate_path)
    country_col = _find_country_column(list(active_df.columns))
    if country_col is None:
        raise HTTPException(status_code=409, detail="无法识别 active 数据集的国家列。")

    active_df[country_col] = active_df[country_col].astype("string").fillna("").str.strip()
    candidate_df[country_col] = candidate_df[country_col].astype("string").fillna("").str.strip()

    regressed_set: set[str] = set()
    for entry in regressed_countries:
        c = str(entry.get("country", "")).strip()
        if c:
            regressed_set.add(c)

    active_countries = set(active_df[country_col].unique())
    candidate_countries = set(candidate_df[country_col].unique())
    missing_from_candidate = active_countries - candidate_countries

    # From candidate: keep rows for non-regressed countries
    candidate_keep = candidate_df[~candidate_df[country_col].isin(regressed_set)].copy()
    # From active: keep rows for regressed countries + countries missing in candidate
    active_keep = active_df[active_df[country_col].isin(regressed_set | missing_from_candidate)].copy()
    candidate_keep, deprecated_static_summary = (
        _carry_forward_deprecated_static_columns(
            active_frame=active_df[
                active_df[country_col].isin(set(candidate_keep[country_col].unique()))
            ],
            candidate_frame=candidate_keep,
        )
    )

    # Align columns: union of all columns from both dataframes
    all_columns = list(dict.fromkeys(list(active_df.columns) + list(candidate_df.columns)))
    for df in (candidate_keep, active_keep):
        for col in all_columns:
            if col not in df.columns:
                df[col] = None

    merged = pd.concat([candidate_keep, active_keep], ignore_index=True)
    return (
        merged[[col for col in all_columns if col in merged.columns]],
        deprecated_static_summary,
    )


def _run_smart_merge(job_id: str) -> None:
    """Background runner: smart merge → rebuild partitions/manifest/fingerprint."""
    state = _load_job_state(job_id)
    if str(state.get("status") or "") == "cancelled":
        _RUNNING_THREADS.pop(job_id, None)
        return
    log_path = _job_log_path(job_id)

    try:
        state["status"] = "running"
        state["phase"] = "smart_merging"
        _persist_job_state(state)
        _append_log(log_path, f"[{_utc_now().isoformat()}] Smart Merge: 开始合并数据...")

        active_paths = _active_data_paths()
        artifacts = state.get("artifacts", {}) or {}
        candidate_path = _project_path(str(artifacts.get("stagingOutputPath") or "").strip())

        if candidate_path is None or not candidate_path.exists():
            raise RuntimeError("找不到 candidate staging parquet。")
        if not active_paths["parquet"].exists():
            raise RuntimeError("找不到 active 数据集，无法执行 Smart Merge。")

        regressions = _find_publish_country_regressions(
            active_parquet_path=active_paths["parquet"],
            candidate_parquet_path=candidate_path,
        )
        if not regressions:
            _append_log(log_path, f"[{_utc_now().isoformat()}] Smart Merge: 无回归国家，不需要合并。")
            state["status"] = "success"
            state["phase"] = "completed"
            state["summaries"] = {
                **(state.get("summaries") or {}),
                "smartMerge": {
                    "mergedAt": _utc_now().isoformat(),
                    "regressedCountryCount": 0,
                    "regressedCountries": [],
                    "totalRowCount": 0,
                },
            }
            _persist_job_state(state)
            return

        merged_df, deprecated_static_summary = _smart_merge_dataframes(
            active_path=active_paths["parquet"],
            candidate_path=candidate_path,
            regressed_countries=regressions,
        )

        # Write merged parquet to staging output (overwrite)
        candidate_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(candidate_path, index=False)
        row_count = len(merged_df)
        regressed_names = sorted(r["country"] for r in regressions if r.get("country"))
        _append_log(
            log_path,
            f"[{_utc_now().isoformat()}] Smart Merge: 合并完成，共 {row_count} 行。"
            f" 回归国家({len(regressed_names)}): {', '.join(regressed_names)}",
        )
        if deprecated_static_summary.get("enabled"):
            inherited_rows = sum(
                int(item.get("inheritedRowCount", 0) or 0)
                for item in deprecated_static_summary.get(
                    "columnResults",
                    {},
                ).values()
                if isinstance(item, dict)
            )
            _append_log(
                log_path,
                f"[{_utc_now().isoformat()}] Smart Merge: 已停用静态字段仅在配置键匹配且 active 旧值一致时沿用，"
                f"columns={len(deprecated_static_summary.get('columns', []))}, "
                f"inheritedCells={inherited_rows}。",
            )

        # Rebuild partition/manifest/fingerprint from merged parquet
        job_dir = _job_dir(job_id)
        staging_dir = candidate_path.parent
        partition_output = _project_path(str(artifacts.get("partitionOutputPath") or "").strip())
        manifest_path = _project_path(str(artifacts.get("manifestPath") or "").strip())
        fingerprint_path = _project_path(str(artifacts.get("fingerprintPath") or "").strip())

        if not REBUILD_SCRIPT_PATH.exists():
            raise RuntimeError(f"找不到重建脚本: {REBUILD_SCRIPT_PATH}")

        rebuild_args = [
            sys.executable,
            str(REBUILD_SCRIPT_PATH),
            "--input-parquet",
            str(candidate_path),
            "--output-dir",
            str(job_dir / "smart_merge"),
            "--partition-output",
            str(partition_output or staging_dir / "partitioned_dataset_v1"),
            "--manifest",
            str(manifest_path or staging_dir / "manifest.json"),
            "--fingerprint",
            str(fingerprint_path or staging_dir / "dataset_fingerprint.json"),
        ]

        state["phase"] = "smart_merge_rebuild"
        _persist_job_state(state)
        _ensure_job_not_cancelled(job_id)
        _run_logged_command(
            label="Smart Merge rebuild",
            args=rebuild_args,
            log_path=log_path,
        )

        state["summaries"] = {
            **(state.get("summaries") or {}),
            "smartMerge": {
                "mergedAt": _utc_now().isoformat(),
                "regressedCountryCount": len(regressions),
                "regressedCountries": regressed_names,
                "totalRowCount": row_count,
                "deprecatedStaticCarryForward": deprecated_static_summary,
            },
        }
        state["status"] = "success"
        state["phase"] = "completed"
        state["finishedAt"] = _utc_now().isoformat()
        _persist_job_state(state)
        _append_log(
            log_path,
            f"[{_utc_now().isoformat()}] Smart Merge: 全部完成。请进入 Review → Publish。",
        )

    except _JobCancelled as exc:
        state = _load_job_state(job_id)
        state["status"] = "cancelled"
        state["phase"] = "cancelled"
        state["finishedAt"] = state.get("finishedAt") or _utc_now().isoformat()
        state["error"] = str(exc)
        state["currentProcess"] = None
        _persist_job_state(state)
        _write_jato_etl_pipeline_status(state)
        _append_log(log_path, f"[{_utc_now().isoformat()}] Cancelled: {exc}")
    except Exception as exc:
        state["status"] = "failed"
        state["phase"] = "smart_merge_failed"
        state["finishedAt"] = _utc_now().isoformat()
        state["error"] = str(exc)
        _persist_job_state(state)
        _append_log(log_path, "\n=== Smart Merge Failed ===")
        _append_log(log_path, str(exc))
        _append_log(log_path, traceback.format_exc())
    finally:
        _RUNNING_THREADS.pop(job_id, None)


def _launch_smart_merge_thread(job_id: str) -> None:
    worker = threading.Thread(
        target=_run_smart_merge,
        args=(job_id,),
        name=f"jato-smart-merge-{job_id}",
        daemon=True,
    )
    _RUNNING_THREADS[job_id] = worker
    worker.start()


def create_smart_merge_candidate(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    """Smart Merge: merge regressed countries from active into candidate, then rebuild artifacts."""
    _require_no_running_monthly_update_jobs(excluding_job_id=job_id)
    payload = _load_job_state(job_id)

    if (
        str(payload.get("status", "")) != "success"
        or str(payload.get("phase", "")) != "completed"
    ):
        raise HTTPException(
            status_code=409,
            detail="只有 success/completed 的月更任务才能执行 Smart Merge。",
        )

    publication = payload.get("publication")
    if isinstance(publication, dict) and publication.get("publishedAt") and not publication.get("rolledBackAt"):
        raise HTTPException(status_code=409, detail="该月更任务已经 publish 过，不能执行 Smart Merge。")

    summaries = payload.get("summaries")
    if isinstance(summaries, dict) and summaries.get("smartMerge") is not None:
        raise HTTPException(status_code=409, detail="该任务已执行过 Smart Merge。")

    active_paths = _active_data_paths()
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务缺少 staging 产物信息。")

    candidate_path = _project_path(str(artifacts.get("stagingOutputPath") or "").strip())
    if candidate_path is None or not candidate_path.exists():
        raise HTTPException(status_code=409, detail="找不到 candidate staging parquet，不能执行 Smart Merge。")
    if not active_paths["parquet"].exists():
        raise HTTPException(status_code=409, detail="找不到 active 数据集，不能执行 Smart Merge。")

    payload["triggeredBy"] = triggered_by.strip() or "anonymous"
    _persist_job_state(payload)

    _launch_smart_merge_thread(job_id)
    return _serialize_job_state(payload, include_log_tail=False)


def _build_runtime_check(payload: dict[str, Any]) -> dict[str, Any]:
    job_id = str(payload.get("jobId") or "")
    pid = _current_process_pid(payload)
    process_alive = _process_exists(pid)
    thread_alive = _thread_is_alive(job_id)
    checked_at = _utc_now().isoformat()
    return {
        "checkedAt": checked_at,
        "statusAtCheck": str(payload.get("status") or ""),
        "phaseAtCheck": str(payload.get("phase") or ""),
        "threadAlive": thread_alive,
        "processPid": pid,
        "processAlive": process_alive,
        "log": _job_log_probe(_job_log_path(job_id)),
        "artifacts": _artifact_probe(payload),
    }


def recheck_jato_monthly_update_job(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    runtime_check = _build_runtime_check(payload)
    payload["runtimeCheck"] = runtime_check

    status = str(payload.get("status") or "")
    if status in RUNNING_JOB_STATUSES:
        current_process = payload.get("currentProcess")
        process_was_expected = isinstance(current_process, dict)
        has_live_worker = bool(runtime_check["threadAlive"] or runtime_check["processAlive"])
        if process_was_expected and not runtime_check["processAlive"]:
            has_live_worker = False

        if not has_live_worker:
            now = _utc_now().isoformat()
            payload["status"] = "failed"
            payload["phase"] = "stale_failed"
            payload["finishedAt"] = now
            payload["currentProcess"] = None
            payload["error"] = (
                "Stale monthly update job: job_state still said running/queued, "
                "but no live background worker or subprocess was found."
            )
            payload["runtimeCheck"] = {
                **runtime_check,
                "resolvedAs": "stale_failed",
                "resolvedBy": triggered_by.strip() or "anonymous",
                "resolvedAt": now,
            }
            _append_log(
                _job_log_path(job_id),
                (
                    f"[{now}] Recheck marked job as stale_failed by "
                    f"{triggered_by.strip() or 'anonymous'}."
                ),
            )
            _write_jato_etl_pipeline_status(payload)

    _persist_job_state(payload)
    return _serialize_job_state(payload, include_log_tail=True)


def cancel_jato_monthly_update_job(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    status = str(payload.get("status") or "")
    if status not in RUNNING_JOB_STATUSES:
        raise HTTPException(
            status_code=409,
            detail="只有 queued/running 的月更任务可以终止。",
        )

    pid = _current_process_pid(payload)
    termination = _terminate_process_group(pid) if pid is not None else {
        "pid": None,
        "sigtermSent": False,
        "sigkillSent": False,
        "processAliveBefore": False,
        "processAliveAfter": False,
        "message": "No current subprocess recorded.",
    }
    now = _utc_now().isoformat()
    actor = triggered_by.strip() or "anonymous"
    phase = str(payload.get("phase") or "unknown")
    payload["status"] = "cancelled"
    payload["phase"] = "cancelled"
    payload["finishedAt"] = now
    payload["error"] = f"Cancelled by {actor} during {phase}"
    payload["currentProcess"] = None
    payload["cancellation"] = {
        "cancelledAt": now,
        "cancelledBy": actor,
        "phaseAtCancel": phase,
        "termination": termination,
    }
    payload["runtimeCheck"] = _build_runtime_check(payload)
    _persist_job_state(payload)
    _write_jato_etl_pipeline_status(payload)
    _append_log(
        _job_log_path(job_id),
        (
            f"[{now}] Cancelled by {actor} during {phase}. "
            f"termination={json.dumps(termination, ensure_ascii=False)}"
        ),
    )
    return _serialize_job_state(payload, include_log_tail=True)


def list_jato_monthly_update_jobs(*, limit: int = 20) -> dict[str, Any]:
    items = [
        _serialize_job_state(payload, include_log_tail=False)
        for payload in _list_job_state_payloads()
    ]

    items.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
    sliced = items[:limit]
    return {"rows": len(sliced), "items": sliced}


def get_jato_monthly_update_job(job_id: str) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    return _serialize_job_state(payload, include_log_tail=True)


def get_jato_monthly_update_maintenance_status() -> dict[str, Any]:
    baseline_path, baseline_source = _resolve_latest_baseline()
    patch_dirs = sorted([path for path in PATCHES_ROOT.glob("*") if path.is_dir()])
    latest_patch_dir = _latest_patch_dir(patch_dirs)
    processed_root = _processed_data_root()
    active_paths = _active_data_paths()
    metrics = [
        _build_storage_metric(
            key="active-baseline",
            label="Active baseline",
            paths=[BASELINE_ROOT],
        ),
        _build_storage_metric(
            key="baseline-archive",
            label="Archived baselines",
            paths=[HISTORY_ARCHIVE_ROOT / "baseline"],
        ),
        _build_storage_metric(
            key="patch-batches",
            label="Patch batches",
            paths=[PATCHES_ROOT],
        ),
        _build_storage_metric(
            key="upload-session-cache",
            label="Upload session cache",
            paths=[_upload_session_root()],
        ),
        _build_storage_metric(
            key="job-upload-copies",
            label="Job upload copies",
            paths=_job_upload_dirs(),
        ),
        _build_storage_metric(
            key="review-reports",
            label="Raw compare reviews",
            paths=[processed_root / "reviews" / "raw_compare"],
        ),
        _build_storage_metric(
            key="staging-outputs",
            label="Staging outputs",
            paths=[processed_root / "staging"],
        ),
        _build_storage_metric(
            key="active-dataset",
            label="Active dataset",
            paths=[
                active_paths["parquet"],
                active_paths["manifest"],
                active_paths["partition"],
                active_paths["fingerprint"],
                active_paths["refreshReport"],
            ],
        ),
        _build_storage_metric(
            key="refresh-backups",
            label="Refresh backups",
            paths=[active_paths["backupRoot"]],
        ),
    ]
    return {
        "checkedAt": _utc_now().isoformat(),
        "activeBaselinePath": _relative_to_project(baseline_path),
        "activeBaselineSource": baseline_source,
        "latestPatchBatch": latest_patch_dir.name if latest_patch_dir is not None else None,
        "jobCount": len(_list_job_state_payloads()),
        "uploadSessionCount": len(_iter_upload_session_payloads()),
        "trackedStorageBytes": sum(int(item["bytes"]) for item in metrics),
        "storageMetrics": metrics,
    }


def promote_current_active_to_baseline(*, triggered_by: str) -> dict[str, Any]:
    running_jobs = [
        str(payload.get("jobId", ""))
        for payload in _list_job_state_payloads()
        if str(payload.get("status", "")) in {"queued", "running"}
    ]
    if running_jobs:
        raise HTTPException(
            status_code=409,
            detail="存在运行中的月更任务，请等待完成后再保存新的 baseline。",
        )

    active_parquet_path = _active_data_paths()["parquet"]
    if not active_parquet_path.exists():
        raise HTTPException(
            status_code=409,
            detail="当前 active parquet 不存在，不能生成 baseline。",
        )

    try:
        frame = _load_dataset_frame(active_parquet_path)
    except Exception as exc:
        raise HTTPException(
            status_code=409,
            detail="读取当前 active parquet 失败，不能生成 baseline。",
        ) from exc

    latest_month = _detect_latest_month_from_dataset_frame(
        frame,
        path_label=active_parquet_path.name,
    )
    country_count = _collect_dataset_country_count(
        frame,
        path_label=active_parquet_path.name,
    )
    row_count = int(len(frame))

    maintenance_dir = MONTHLY_UPDATE_JOB_ROOT / "_maintenance"
    maintenance_dir.mkdir(parents=True, exist_ok=True)
    temp_export_path = maintenance_dir / f"baseline-export-{uuid4().hex}.xlsx"
    try:
        frame.to_excel(
            temp_export_path,
            index=False,
            sheet_name=DEFAULT_UPLOAD_SHEET_NAME,
            engine="openpyxl",
        )
    except Exception as exc:
        temp_export_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=500,
            detail="导出 baseline xlsx 失败，请确认后端具备 openpyxl 能力。",
        ) from exc

    BASELINE_ROOT.mkdir(parents=True, exist_ok=True)
    archived_baselines: list[str] = []
    for path in _list_supported_excel_files(BASELINE_ROOT):
        target = _move_to_archive(path, HISTORY_ARCHIVE_ROOT / "baseline")
        archived_baselines.append(_relative_to_project(target) or str(target))

    baseline_path = BASELINE_ROOT / _baseline_snapshot_filename(
        latest_month=latest_month,
        country_count=country_count,
    )
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(temp_export_path), str(baseline_path))

    return {
        "promotedAt": _utc_now().isoformat(),
        "triggeredBy": triggered_by.strip() or "anonymous",
        "sourceParquetPath": _relative_to_project(active_parquet_path),
        "baselinePath": _relative_to_project(baseline_path),
        "detectedLatestMonth": latest_month,
        "countryCount": country_count,
        "rowCount": row_count,
        "archivedBaselineCount": len(archived_baselines),
        "archivedBaselines": archived_baselines,
    }


def run_jato_monthly_update_cleanup(*, triggered_by: str, cleanup_tier: str = SAFE_CLEANUP_TIER) -> dict[str, Any]:
    normalized_cleanup_tier = _normalize_cleanup_tier(cleanup_tier)
    job_payloads = _list_job_state_payloads()
    running_jobs = [
        str(payload.get("jobId", ""))
        for payload in job_payloads
        if str(payload.get("status", "")) in {"queued", "running"}
    ]
    if running_jobs:
        raise HTTPException(
            status_code=409,
            detail="存在运行中的月更任务，暂时不能执行一键清理。",
        )

    baseline_files = _list_supported_excel_files(BASELINE_ROOT)
    latest_baseline = _latest_baseline_file(baseline_files)
    archived_baselines: list[str] = []
    if latest_baseline is not None:
        for path in baseline_files:
            if path == latest_baseline:
                continue
            target = _move_to_archive(path, HISTORY_ARCHIVE_ROOT / "baseline")
            archived_baselines.append(_relative_to_project(target) or str(target))

    patch_dirs = sorted([path for path in PATCHES_ROOT.glob("*") if path.is_dir()])
    latest_patch_dir = _latest_patch_dir(patch_dirs)
    archived_patch_dirs: list[str] = []
    if latest_patch_dir is not None:
        for path in patch_dirs:
            if path == latest_patch_dir:
                continue
            target = _move_to_archive(path, HISTORY_ARCHIVE_ROOT / "patches")
            archived_patch_dirs.append(_relative_to_project(target) or str(target))

    removed_job_upload_dirs: list[str] = []
    removed_job_upload_bytes = 0
    for payload in job_payloads:
        if str(payload.get("status", "")) not in {"success", "failed"}:
            continue
        job_id = str(payload.get("jobId", "")).strip()
        if not job_id:
            continue
        upload_dir = _job_dir(job_id) / "uploads"
        if not upload_dir.exists():
            continue
        path_bytes, _, _ = _measure_path_usage(upload_dir)
        removed_job_upload_bytes += path_bytes
        shutil.rmtree(upload_dir)
        removed_job_upload_dirs.append(_relative_to_project(upload_dir) or str(upload_dir))
        upload_payload = payload.get("upload")
        if isinstance(upload_payload, dict):
            upload_payload["storedPath"] = None
        _persist_job_state(payload)

    removed_upload_session_dirs, removed_upload_session_bytes = _remove_cleanup_paths(
        _child_cleanup_paths(_upload_session_root())
    )
    deleted_review_dirs: list[str] = []
    deleted_review_bytes = 0
    deleted_staging_dirs: list[str] = []
    deleted_staging_bytes = 0
    deleted_refresh_backup_dirs: list[str] = []
    deleted_refresh_backup_bytes = 0
    deleted_archived_baselines: list[str] = []
    deleted_archived_baseline_bytes = 0
    deleted_archived_patch_dirs: list[str] = []
    deleted_archived_patch_bytes = 0
    if normalized_cleanup_tier == CAUTIOUS_CLEANUP_TIER:
        processed_root = _processed_data_root()
        active_paths = _active_data_paths()
        deleted_review_dirs, deleted_review_bytes = _remove_cleanup_paths(
            _child_cleanup_paths(processed_root / "reviews" / "raw_compare")
        )
        deleted_staging_dirs, deleted_staging_bytes = _remove_cleanup_paths(
            _child_cleanup_paths(processed_root / "staging")
        )
        deleted_refresh_backup_dirs, deleted_refresh_backup_bytes = _remove_cleanup_paths(
            _child_cleanup_paths(active_paths["backupRoot"])
        )
        deleted_archived_baselines, deleted_archived_baseline_bytes = _remove_cleanup_paths(
            _list_supported_excel_files(HISTORY_ARCHIVE_ROOT / "baseline")
        )
        deleted_archived_patch_dirs, deleted_archived_patch_bytes = _remove_cleanup_paths(
            [path for path in _child_cleanup_paths(HISTORY_ARCHIVE_ROOT / "patches") if path.is_dir()]
        )

    cleaned_at = _utc_now().isoformat()
    freed_bytes = (
        removed_job_upload_bytes
        + removed_upload_session_bytes
        + deleted_review_bytes
        + deleted_staging_bytes
        + deleted_refresh_backup_bytes
        + deleted_archived_baseline_bytes
        + deleted_archived_patch_bytes
    )
    return {
        "cleanedAt": cleaned_at,
        "triggeredBy": triggered_by.strip() or "anonymous",
        "cleanupTier": normalized_cleanup_tier,
        "activeBaselinePath": _relative_to_project(latest_baseline),
        "activePatchMonth": latest_patch_dir.name if latest_patch_dir is not None else None,
        "freedBytes": int(freed_bytes),
        "archivedBaselineCount": len(archived_baselines),
        "archivedBaselines": archived_baselines,
        "archivedPatchDirCount": len(archived_patch_dirs),
        "archivedPatchDirs": archived_patch_dirs,
        "removedUploadSessionDirCount": len(removed_upload_session_dirs),
        "removedUploadSessionDirs": removed_upload_session_dirs,
        "removedJobUploadDirCount": len(removed_job_upload_dirs),
        "removedJobUploadDirs": removed_job_upload_dirs,
        "deletedReviewDirCount": len(deleted_review_dirs),
        "deletedReviewDirs": deleted_review_dirs,
        "deletedStagingDirCount": len(deleted_staging_dirs),
        "deletedStagingDirs": deleted_staging_dirs,
        "deletedRefreshBackupDirCount": len(deleted_refresh_backup_dirs),
        "deletedRefreshBackupDirs": deleted_refresh_backup_dirs,
        "deletedArchivedBaselineCount": len(deleted_archived_baselines),
        "deletedArchivedBaselines": deleted_archived_baselines,
        "deletedArchivedPatchDirCount": len(deleted_archived_patch_dirs),
        "deletedArchivedPatchDirs": deleted_archived_patch_dirs,
    }
