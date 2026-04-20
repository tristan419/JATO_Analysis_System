import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from elt_worker import (
    DEFAULT_SHEET,
    MONTH_COL_PATTERN,
    YEAR_COL_PATTERN,
    normalize_dataframe,
    parse_csv_list,
    read_excel_with_fallback,
    resolve_explicit_file,
    to_project_relative,
)
from logging_utils import build_job_id, get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "04_Processed_data" / "reviews" / "raw_compare"
)
DEFAULT_SAMPLE_LIMIT = 20
HIGH_CHANGE_RATE_THRESHOLD = 0.25
HIGH_CHANGE_RECORD_THRESHOLD = 100

DEFAULT_COUNTRY_CANDIDATES = ("国家", "country")
DEFAULT_COMPARE_KEY_GROUPS = [
    ("country", ("国家", "country"), True),
    ("make", ("品牌", "make"), False),
    ("model", ("model",), False),
    ("version", ("version name", "versionname"), False),
    ("powertrain", ("动总规整", "powertrain"), False),
    ("segment", ("细分市场（按车长）", "细分市场", "segment"), False),
]


def sanitize_identifier(text: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "-", str(text).strip())
    normalized = normalized.strip("-").lower()
    return normalized or "compare"


def normalize_scalar(value: Any) -> str:
    if pd.isna(value):
        return "<null>"
    if isinstance(value, str):
        return value.strip()
    return str(value)


def normalize_series_for_digest(series: pd.Series) -> pd.Series:
    if (
        pd.api.types.is_string_dtype(series.dtype)
        or pd.api.types.is_object_dtype(series.dtype)
    ):
        return series.astype("string").fillna("<null>").str.strip()
    return series.map(normalize_scalar).astype("string")


def normalize_frame_for_digest(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in normalized.columns:
        normalized[column] = normalize_series_for_digest(normalized[column])
    return normalized


def to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): to_json_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_safe(item) for item in value]
    if pd.isna(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def ensure_output_dir(path_text: str | None, compare_id: str) -> Path:
    if path_text:
        output_dir = Path(path_text)
        if not output_dir.is_absolute():
            output_dir = PROJECT_ROOT / output_dir
    else:
        output_dir = DEFAULT_OUTPUT_ROOT / compare_id
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def column_lookup(columns: list[str]) -> dict[str, str]:
    return {
        str(column).strip().lower(): str(column)
        for column in columns
    }


def find_column_match(
    columns: list[str],
    candidates: tuple[str, ...],
) -> str | None:
    lookup = column_lookup(columns)
    for candidate in candidates:
        target = lookup.get(str(candidate).strip().lower())
        if target:
            return target
    return None


def detect_time_columns(columns: list[str]) -> tuple[list[str], list[str]]:
    year_columns = [
        column
        for column in columns
        if YEAR_COL_PATTERN.fullmatch(str(column))
    ]
    month_columns = [
        column
        for column in columns
        if MONTH_COL_PATTERN.match(str(column))
    ]
    return sort_time_labels(year_columns), sort_time_labels(month_columns)


def time_sort_key(label: str) -> tuple[int, int, str]:
    text = str(label).strip()
    if MONTH_COL_PATTERN.match(text):
        parsed = datetime.strptime(text.title(), "%Y %b")
        return (parsed.year, parsed.month, text)
    if YEAR_COL_PATTERN.fullmatch(text):
        return (int(text), 0, text)
    return (9999, 12, text)


def sort_time_labels(labels: list[str]) -> list[str]:
    normalized = {
        str(label).strip()
        for label in labels
        if str(label).strip()
    }
    return sorted(normalized, key=time_sort_key)


def month_distance(old_label: str | None, new_label: str | None) -> int | None:
    if not old_label or not new_label:
        return None
    old_key = time_sort_key(old_label)
    new_key = time_sort_key(new_label)
    return (new_key[0] - old_key[0]) * 12 + (new_key[1] - old_key[1])


def series_has_data(series: pd.Series) -> bool:
    if series.empty:
        return False
    if (
        pd.api.types.is_string_dtype(series.dtype)
        or pd.api.types.is_object_dtype(series.dtype)
    ):
        normalized = series.astype("string").fillna("").str.strip()
        return bool((normalized != "").any())
    return bool(series.notna().any())


def normalize_country_name(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def collect_country_set(df: pd.DataFrame, country_col: str) -> list[str]:
    countries = {
        normalize_country_name(value)
        for value in df[country_col].astype("string")
        if normalize_country_name(value)
    }
    return sorted(countries)


def collect_country_time_info(
    df: pd.DataFrame,
    country_col: str,
    time_columns: list[str],
) -> dict[str, dict[str, Any]]:
    info: dict[str, dict[str, Any]] = {}
    grouped = df.groupby(country_col, dropna=False, sort=False)
    for raw_country, group_df in grouped:
        country = normalize_country_name(raw_country)
        if not country:
            continue
        present_months = [
            column
            for column in time_columns
            if column in group_df.columns and series_has_data(group_df[column])
        ]
        sorted_months = sort_time_labels(present_months)
        info[country] = {
            "months": sorted_months,
            "latestMonth": sorted_months[-1] if sorted_months else None,
            "rowCount": int(len(group_df)),
        }
    return info


def build_compare_plan(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    country_col_text: str,
    explicit_compare_keys: list[str],
) -> dict[str, Any]:
    old_columns = [str(column) for column in old_df.columns]
    new_columns = [str(column) for column in new_df.columns]
    renamed_candidates: list[dict[str, str]] = []
    missing_critical: list[str] = []
    groups: list[dict[str, str]] = []
    compare_key_mode = "explicit" if explicit_compare_keys else "auto"

    if explicit_compare_keys:
        requested_keys = list(
            dict.fromkeys([country_col_text, *explicit_compare_keys])
        )
        for requested in requested_keys:
            old_match = find_column_match(old_columns, (requested,))
            new_match = find_column_match(new_columns, (requested,))
            if not old_match or not new_match:
                missing_critical.append(requested)
                continue
            groups.append(
                {
                    "id": sanitize_identifier(requested),
                    "display": requested,
                    "oldColumn": old_match,
                    "newColumn": new_match,
                }
            )
    else:
        for group_id, candidates, required in DEFAULT_COMPARE_KEY_GROUPS:
            if group_id == "country":
                merged_candidates = tuple(
                    dict.fromkeys((country_col_text, *candidates))
                )
            else:
                merged_candidates = candidates
            old_match = find_column_match(old_columns, merged_candidates)
            new_match = find_column_match(new_columns, merged_candidates)
            if old_match and new_match:
                display = (
                    old_match
                    if old_match == new_match
                    else f"{old_match}|{new_match}"
                )
                groups.append(
                    {
                        "id": group_id,
                        "display": display,
                        "oldColumn": old_match,
                        "newColumn": new_match,
                    }
                )
                if old_match != new_match:
                    renamed_candidates.append(
                        {
                            "group": group_id,
                            "oldColumn": old_match,
                            "newColumn": new_match,
                        }
                    )
                continue
            if required:
                missing_critical.append(group_id)

    return {
        "compareKeyMode": compare_key_mode,
        "groups": groups,
        "compareKeyColumns": [group["display"] for group in groups],
        "missingCriticalColumns": sorted(set(missing_critical)),
        "renamedCandidates": renamed_candidates,
    }


def build_file_summary(
    input_path: Path,
    df: pd.DataFrame,
    country_col: str,
    time_columns: list[str],
    sheet_name: str,
) -> dict[str, Any]:
    file_stat = input_path.stat()
    earliest = time_columns[0] if time_columns else None
    latest = time_columns[-1] if time_columns else None
    return {
        "path": to_project_relative(input_path),
        "fileName": input_path.name,
        "bytes": int(file_stat.st_size),
        "mtimeUtc": datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc,
        ).isoformat(),
        "sheetName": sheet_name,
        "rowCount": int(len(df)),
        "columnCount": int(len(df.columns)),
        "countryCount": int(len(collect_country_set(df, country_col))),
        "monthColumnCount": int(len(time_columns)),
        "earliestMonth": earliest,
        "latestMonth": latest,
    }


def build_schema_check(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    compare_plan: dict[str, Any],
) -> dict[str, Any]:
    old_columns = {str(column) for column in old_df.columns}
    new_columns = {str(column) for column in new_df.columns}
    missing_columns = sorted(old_columns - new_columns)
    added_columns = sorted(new_columns - old_columns)
    critical_missing = list(compare_plan["missingCriticalColumns"])
    status = "fail" if critical_missing else "pass"
    if (
        status == "pass"
        and (added_columns or compare_plan["renamedCandidates"])
    ):
        status = "review"
    return {
        "status": status,
        "missingColumns": missing_columns,
        "addedColumns": added_columns,
        "renamedCandidates": compare_plan["renamedCandidates"],
        "incompatibleColumns": [],
        "criticalMissingColumns": critical_missing,
    }


def build_time_axis_check(
    old_time_columns: list[str],
    new_time_columns: list[str],
) -> dict[str, Any]:
    added_months = sort_time_labels(
        list(set(new_time_columns) - set(old_time_columns))
    )
    removed_months = sort_time_labels(
        list(set(old_time_columns) - set(new_time_columns))
    )
    overlapping_months = sort_time_labels(
        list(set(old_time_columns) & set(new_time_columns))
    )
    latest_old = old_time_columns[-1] if old_time_columns else None
    latest_new = new_time_columns[-1] if new_time_columns else None
    latest_advanced = False
    if latest_old and latest_new:
        latest_advanced = bool(
            time_sort_key(latest_new) > time_sort_key(latest_old)
        )
    return {
        "recognizedMonthColumnsOld": old_time_columns,
        "recognizedMonthColumnsNew": new_time_columns,
        "addedMonths": added_months,
        "removedMonths": removed_months,
        "overlappingMonths": overlapping_months,
        "latestMonthOld": latest_old,
        "latestMonthNew": latest_new,
        "latestMonthAdvanced": latest_advanced,
    }


def summarize_country_freshness(
    old_info: dict[str, dict[str, Any]],
    new_info: dict[str, dict[str, Any]],
    *,
    allow_missing_countries: bool = False,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    all_countries = sorted(set(old_info) | set(new_info))
    for country in all_countries:
        old_payload = old_info.get(country, {})
        new_payload = new_info.get(country, {})
        if allow_missing_countries and country in old_info and country not in new_info:
            new_payload = old_payload
        old_latest = old_payload.get("latestMonth")
        new_latest = new_payload.get("latestMonth")

        if country not in old_info:
            status = "new_country"
        elif country not in new_info:
            status = (
                "unchanged_latest"
                if allow_missing_countries
                else "missing_in_candidate"
            )
        elif (
            old_latest
            and new_latest
            and time_sort_key(new_latest) > time_sort_key(old_latest)
        ):
            status = "advanced"
        elif (
            old_latest
            and new_latest
            and time_sort_key(new_latest) < time_sort_key(old_latest)
        ):
            status = "regressed"
        else:
            status = "unchanged_latest"

        old_rows = int(old_payload.get("rowCount", 0))
        new_rows = int(new_payload.get("rowCount", 0))
        entries.append(
            {
                "country": country,
                "oldLatestMonth": old_latest,
                "newLatestMonth": new_latest,
                "freshnessStatus": status,
                "freshnessDeltaMonths": month_distance(old_latest, new_latest),
                "oldRowCount": old_rows,
                "newRowCount": new_rows,
                "rowDelta": int(new_rows - old_rows),
            }
        )
    return entries


def summarize_country_coverage(
    old_info: dict[str, dict[str, Any]],
    new_info: dict[str, dict[str, Any]],
    *,
    allow_missing_countries: bool = False,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    all_countries = sorted(set(old_info) | set(new_info))
    for country in all_countries:
        old_payload = old_info.get(country, {})
        new_payload = new_info.get(country, {})
        if allow_missing_countries and country in old_info and country not in new_info:
            new_payload = old_payload
        old_months = list(old_payload.get("months", []))
        new_months = list(new_payload.get("months", []))
        added_months = sort_time_labels(
            list(set(new_months) - set(old_months))
        )
        removed_months = sort_time_labels(
            list(set(old_months) - set(new_months))
        )
        overlapping_months = sort_time_labels(
            list(set(old_months) & set(new_months))
        )

        if (
            country not in old_info
            or (
                country not in new_info
                and not allow_missing_countries
            )
        ):
            status = "scope_changed"
        elif removed_months:
            status = "regressed_coverage"
        elif added_months:
            latest_old = old_months[-1] if old_months else None
            latest_added = added_months[-1] if added_months else None
            if (
                latest_old
                and latest_added
                and time_sort_key(latest_added) > time_sort_key(latest_old)
            ):
                status = "added_future_months"
            else:
                status = "backfill_old_months"
        elif old_months == new_months:
            status = "unchanged_coverage"
        else:
            status = "revised_overlap_only"

        entries.append(
            {
                "country": country,
                "oldMonths": old_months,
                "newMonths": new_months,
                "addedMonths": added_months,
                "removedMonths": removed_months,
                "overlappingMonths": overlapping_months,
                "coverageStatus": status,
            }
        )
    return entries


def compute_payload_digest(payload_df: pd.DataFrame) -> str:
    if payload_df.empty or not list(payload_df.columns):
        return "0"
    normalized = normalize_frame_for_digest(payload_df)
    hash_values = pd.util.hash_pandas_object(
        normalized,
        index=False,
        categorize=True,
    )
    checksum = int(hash_values.astype("uint64").sum())
    return str(checksum)


def build_key_digest_frame(
    df: pd.DataFrame,
    key_groups: list[dict[str, str]],
    source_name: str,
    payload_columns: list[str],
) -> pd.DataFrame:
    key_columns = [group[f"{source_name}Column"] for group in key_groups]
    rename_map = {
        group[f"{source_name}Column"]: group["id"]
        for group in key_groups
    }
    key_ids = list(rename_map.values())
    selected_keys = normalize_frame_for_digest(
        df[key_columns].copy().rename(columns=rename_map)
    )
    grouped = selected_keys.groupby(
        key_ids,
        dropna=False,
        sort=False,
    )
    result = grouped.size().rename("recordCount").reset_index()
    result["recordCount"] = result["recordCount"].astype(int)
    result["multiRow"] = result["recordCount"] > 1

    if not payload_columns:
        result["payloadDigest"] = "0"
        result["samplePayload"] = [{} for _ in range(len(result))]
        return result[
            [*key_ids, "payloadDigest", "recordCount", "multiRow", "samplePayload"]
        ]

    payload_df = normalize_frame_for_digest(df[payload_columns].copy())
    payload_hashes = pd.util.hash_pandas_object(
        payload_df,
        index=False,
        categorize=True,
    ).astype("uint64")

    digest_source = selected_keys.copy()
    digest_source["__payload_hash"] = payload_hashes
    payload_sums = digest_source.groupby(
        key_ids,
        dropna=False,
        sort=False,
    )["__payload_hash"].sum().reset_index(name="payloadHashSum")
    payload_sums["payloadDigest"] = payload_sums["payloadHashSum"].map(
        lambda value: str(int(value))
    )

    sample_source = selected_keys.copy()
    for column in payload_columns:
        sample_source[column] = payload_df[column]
    sample_first = sample_source.groupby(
        key_ids,
        dropna=False,
        sort=False,
    ).first().reset_index()
    sample_payloads = sample_first[payload_columns].to_dict("records")
    sample_frame = sample_first[key_ids].copy()
    sample_frame["samplePayload"] = sample_payloads

    result = result.merge(
        payload_sums[key_ids + ["payloadDigest"]],
        on=key_ids,
        how="left",
        sort=False,
    )
    result = result.merge(
        sample_frame,
        on=key_ids,
        how="left",
        sort=False,
    )
    return result[
        [*key_ids, "payloadDigest", "recordCount", "multiRow", "samplePayload"]
    ]


def compute_changed_fields(
    old_payload: dict[str, Any],
    new_payload: dict[str, Any],
) -> list[str]:
    changed_fields: list[str] = []
    all_keys = sorted(set(old_payload) | set(new_payload))
    for key in all_keys:
        if (
            normalize_scalar(old_payload.get(key))
            != normalize_scalar(new_payload.get(key))
        ):
            changed_fields.append(key)
    return changed_fields


def normalize_country_series(df: pd.DataFrame, country_col: str) -> pd.Series:
    return df[country_col].astype("string").fillna("").str.strip()


def summarize_overlap_changes(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    compare_plan: dict[str, Any],
    old_country_col: str,
    new_country_col: str,
    coverage_entries: list[dict[str, Any]],
    old_time_columns: list[str],
    new_time_columns: list[str],
    sample_limit: int,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summaries: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    key_groups = list(compare_plan["groups"])
    old_country_series = normalize_country_series(old_df, old_country_col)
    new_country_series = normalize_country_series(new_df, new_country_col)
    old_country_set = set(old_country_series[old_country_series != ""])
    new_country_set = set(new_country_series[new_country_series != ""])
    common_payload_columns = sorted(
        (set(old_df.columns) & set(new_df.columns))
        - set(old_time_columns)
        - set(new_time_columns)
        - {group["oldColumn"] for group in key_groups}
        - {group["newColumn"] for group in key_groups}
    )

    total_countries = len(coverage_entries)
    for country_index, coverage_entry in enumerate(coverage_entries, start=1):
        country = str(coverage_entry["country"])
        if country not in old_country_set:
            continue
        if country not in new_country_set:
            continue
        if progress_callback:
            progress_callback(
                f"🔍 记录级比对 {country} ({country_index}/{total_countries})"
            )

        compare_months = sort_time_labels(
            list(coverage_entry.get("overlappingMonths", []))
        )
        payload_columns = [
            *common_payload_columns,
            *[
                column
                for column in compare_months
                if column in old_df.columns and column in new_df.columns
            ],
        ]

        old_country_df = old_df[old_country_series == country].copy()
        new_country_df = new_df[new_country_series == country].copy()
        old_digest_df = build_key_digest_frame(
            old_country_df,
            key_groups,
            "old",
            payload_columns,
        )
        new_digest_df = build_key_digest_frame(
            new_country_df,
            key_groups,
            "new",
            payload_columns,
        )

        key_ids = [group["id"] for group in key_groups]
        merged = old_digest_df.merge(
            new_digest_df,
            on=key_ids,
            how="outer",
            suffixes=("Old", "New"),
            indicator=True,
        )
        added_mask = merged["_merge"] == "right_only"
        removed_mask = merged["_merge"] == "left_only"
        shared_mask = merged["_merge"] == "both"
        changed_mask = shared_mask & (
            merged["payloadDigestOld"] != merged["payloadDigestNew"]
        )
        unchanged_mask = shared_mask & (
            merged["payloadDigestOld"] == merged["payloadDigestNew"]
        )

        added_count = int(added_mask.sum())
        removed_count = int(removed_mask.sum())
        changed_count = int(changed_mask.sum())
        unchanged_count = int(unchanged_mask.sum())
        total_union = int(len(merged))
        diff_total = added_count + removed_count + changed_count
        change_rate = round(diff_total / max(total_union, 1), 4)

        sample_added_keys = merged.loc[added_mask, key_ids].head(
            sample_limit
        ).to_dict("records")
        sample_removed_keys = merged.loc[removed_mask, key_ids].head(
            sample_limit
        ).to_dict("records")
        sample_changed_keys = merged.loc[changed_mask, key_ids].head(
            sample_limit
        ).to_dict("records")

        changed_rows = merged.loc[changed_mask].head(
            max(sample_limit - len(samples), 0)
        )
        for _, changed_row in changed_rows.iterrows():
            if len(samples) >= sample_limit:
                break
            business_key = {
                group_id: normalize_scalar(changed_row[group_id])
                for group_id in key_ids
            }
            if (
                bool(changed_row.get("multiRowOld"))
                or bool(changed_row.get("multiRowNew"))
            ):
                changed_fields = ["<multi_row_digest_changed>"]
            else:
                old_payload = (
                    to_json_safe(changed_row.get("samplePayloadOld", {}))
                    or {}
                )
                new_payload = (
                    to_json_safe(changed_row.get("samplePayloadNew", {}))
                    or {}
                )
                changed_fields = compute_changed_fields(
                    old_payload,
                    new_payload,
                )
            samples.append(
                {
                    "country": country,
                    "businessKey": business_key,
                    "oldValueDigest": normalize_scalar(
                        changed_row.get("payloadDigestOld")
                    ),
                    "newValueDigest": normalize_scalar(
                        changed_row.get("payloadDigestNew")
                    ),
                    "changedFields": changed_fields,
                }
            )

        summaries.append(
            {
                "country": country,
                "compareMonths": compare_months,
                "compareKeyColumns": list(compare_plan["compareKeyColumns"]),
                "addedRecordCount": added_count,
                "removedRecordCount": removed_count,
                "changedRecordCount": changed_count,
                "unchangedRecordCount": unchanged_count,
                "changeRate": change_rate,
                "sampleAddedKeys": to_json_safe(sample_added_keys),
                "sampleRemovedKeys": to_json_safe(sample_removed_keys),
                "sampleChangedKeys": to_json_safe(sample_changed_keys),
            }
        )

    return summaries, samples


def build_country_scope_summary(
    freshness_entries: list[dict[str, Any]],
    coverage_entries: list[dict[str, Any]],
    old_countries: list[str],
    new_countries: list[str],
    *,
    allow_missing_countries: bool = False,
) -> dict[str, Any]:
    coverage_by_country = {
        str(entry["country"]): str(entry["coverageStatus"])
        for entry in coverage_entries
    }
    changed_countries = sorted(
        {
            str(entry["country"])
            for entry in freshness_entries
            if str(entry["freshnessStatus"]) != "unchanged_latest"
            or int(entry.get("rowDelta", 0)) != 0
            or coverage_by_country.get(
                str(entry["country"]),
                "",
            ) != "unchanged_coverage"
        }
    )
    old_set = set(old_countries)
    new_set = set(new_countries)
    effective_new_set = old_set | new_set if allow_missing_countries else new_set
    overlapping = sorted(old_set & effective_new_set)
    return {
        "oldCountries": old_countries,
        "newCountries": (
            sorted(effective_new_set)
            if allow_missing_countries
            else new_countries
        ),
        "addedCountries": sorted(new_set - old_set),
        "removedCountries": (
            [] if allow_missing_countries else sorted(old_set - new_set)
        ),
        "overlappingCountries": overlapping,
        "unchangedCountryCount": int(
            len(
                [
                    country
                    for country in overlapping
                    if country not in changed_countries
                ]
            )
        ),
        "changedCountryCount": int(len(changed_countries)),
        "changedCountries": changed_countries,
    }


def build_review_findings(
    schema_check: dict[str, Any],
    time_axis_check: dict[str, Any],
    compare_plan: dict[str, Any],
    scope_summary: dict[str, Any],
    freshness_entries: list[dict[str, Any]],
    coverage_entries: list[dict[str, Any]],
    overlap_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    def add_finding(
        severity: str,
        scope: str,
        target: str,
        rule_id: str,
        message: str,
        metrics: dict[str, Any] | None = None,
        suggested_action: str | None = None,
    ) -> None:
        findings.append(
            {
                "severity": severity,
                "scope": scope,
                "target": target,
                "ruleId": rule_id,
                "message": message,
                "metrics": metrics or {},
                "suggestedAction": suggested_action or "review",
            }
        )

    if schema_check["criticalMissingColumns"]:
        add_finding(
            "blocker",
            "global",
            "schema",
            "R003",
            "候选文件缺少关键 compare 字段。",
            {"criticalMissingColumns": schema_check["criticalMissingColumns"]},
            "reject_input_batch",
        )
    if not time_axis_check["recognizedMonthColumnsNew"]:
        add_finding(
            "blocker",
            "global",
            "timeAxis",
            "R002",
            "候选文件未识别到可比较的时间列。",
            {},
            "reject_input_batch",
        )
    if time_axis_check["removedMonths"]:
        add_finding(
            "blocker",
            "global",
            "timeAxis",
            "R005",
            "候选文件缺少 baseline 已存在的月份列。",
            {"removedMonths": time_axis_check["removedMonths"]},
            "reject_input_batch",
        )
    if scope_summary["removedCountries"]:
        add_finding(
            "blocker",
            "global",
            "countryScope",
            "R004",
            "候选文件丢失了 baseline 已存在的国家。",
            {"removedCountries": scope_summary["removedCountries"]},
            "reject_input_batch",
        )
    if len(compare_plan["groups"]) < 2:
        add_finding(
            "blocker",
            "global",
            "compareKeys",
            "R003",
            "可用 compare keys 不足，无法做可信记录级比对。",
            {"compareKeyColumns": compare_plan["compareKeyColumns"]},
            "reject_input_batch",
        )
    elif len(compare_plan["groups"]) < 4:
        add_finding(
            "review",
            "global",
            "compareKeys",
            "R101",
            "自动识别到的 compare keys 不够完整，建议人工确认业务键。",
            {"compareKeyColumns": compare_plan["compareKeyColumns"]},
            "manual_review_required",
        )

    if schema_check["addedColumns"]:
        add_finding(
            "review",
            "global",
            "schema",
            "R101",
            "候选文件新增了 baseline 中不存在的列。",
            {"addedColumns": schema_check["addedColumns"]},
            "manual_review_required",
        )
    if schema_check["renamedCandidates"]:
        add_finding(
            "review",
            "global",
            "schema",
            "R101",
            "检测到语义相同但列名不同的字段映射。",
            {"renamedCandidates": schema_check["renamedCandidates"]},
            "manual_review_required",
        )

    for entry in freshness_entries:
        status = str(entry["freshnessStatus"])
        country = str(entry["country"])
        if status == "advanced":
            add_finding(
                "info",
                "country",
                country,
                "R201",
                "国家最新月份已推进。",
                {
                    "oldLatestMonth": entry["oldLatestMonth"],
                    "newLatestMonth": entry["newLatestMonth"],
                },
                "proceed_to_candidate_refresh",
            )
        elif (
            status == "unchanged_latest"
            and int(entry.get("rowDelta", 0)) != 0
        ):
            add_finding(
                "review",
                "country",
                country,
                "R202",
                "国家最新月份未推进，但行数发生变化。",
                {"rowDelta": entry["rowDelta"]},
                "manual_review_required",
            )
        elif status == "regressed":
            add_finding(
                "blocker",
                "country",
                country,
                "R005",
                "国家最新月份发生回退。",
                {
                    "oldLatestMonth": entry["oldLatestMonth"],
                    "newLatestMonth": entry["newLatestMonth"],
                },
                "reject_input_batch",
            )
        elif status == "new_country":
            add_finding(
                "review",
                "country",
                country,
                "R203",
                "候选文件新增国家。",
                {},
                "manual_review_required",
            )
        elif status == "missing_in_candidate":
            add_finding(
                "blocker",
                "country",
                country,
                "R004",
                "baseline 国家未出现在候选文件中。",
                {},
                "reject_input_batch",
            )

    for entry in coverage_entries:
        status = str(entry["coverageStatus"])
        country = str(entry["country"])
        if status == "backfill_old_months":
            add_finding(
                "review",
                "country",
                country,
                "R103",
                "国家存在旧月份补录。",
                {"addedMonths": entry["addedMonths"]},
                "manual_review_required",
            )
        elif status == "revised_overlap_only":
            add_finding(
                "review",
                "country",
                country,
                "R103",
                "国家只修订了重叠月份，无未来月份推进。",
                {"overlappingMonths": entry["overlappingMonths"]},
                "manual_review_required",
            )
        elif status == "regressed_coverage":
            add_finding(
                "blocker",
                "country",
                country,
                "R204",
                "国家月份覆盖发生退化。",
                {"removedMonths": entry["removedMonths"]},
                "reject_input_batch",
            )

    for summary in overlap_summaries:
        if (
            float(summary["changeRate"]) >= HIGH_CHANGE_RATE_THRESHOLD
            and int(summary["changedRecordCount"])
            >= HIGH_CHANGE_RECORD_THRESHOLD
        ):
            add_finding(
                "review",
                "country",
                str(summary["country"]),
                "R205",
                "国家重叠范围的记录级变化比例偏高。",
                {
                    "changeRate": summary["changeRate"],
                    "changedRecordCount": summary["changedRecordCount"],
                },
                "manual_review_required",
            )

    severity_order = {"blocker": 0, "review": 1, "info": 2}
    return sorted(
        findings,
        key=lambda item: (
            severity_order[item["severity"]],
            item["target"],
            item["ruleId"],
        ),
    )


def decide_review_outcome(findings: list[dict[str, Any]]) -> str:
    severities = {str(finding["severity"]) for finding in findings}
    if "blocker" in severities:
        return "reject_input_batch"
    if "review" in severities:
        return "manual_review_required"
    return "proceed_to_candidate_refresh"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        pd.DataFrame().to_csv(path, index=False, encoding="utf-8-sig")
        return
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def build_review_checklist_markdown(
    report: dict[str, Any],
) -> str:
    findings = list(report["reviewFindings"])
    blocker_findings = [
        finding for finding in findings if finding["severity"] == "blocker"
    ]
    review_findings = [
        finding for finding in findings if finding["severity"] == "review"
    ]
    freshness_entries = list(report["countryFreshnessSummary"])
    advanced = [
        entry
        for entry in freshness_entries
        if entry["freshnessStatus"] == "advanced"
    ]
    unchanged = [
        entry
        for entry in freshness_entries
        if entry["freshnessStatus"] == "unchanged_latest"
    ]

    lines = [
        f"# Raw Compare Review Checklist · {report['compareId']}",
        "",
        "## 1. 本次 compare 摘要",
        "",
        f"- 决策建议：{report['decisionSuggestion']}",
        f"- baseline：{report['baselineInput']['path']}",
        f"- candidate：{report['candidateInput']['path']}",
        f"- compare keys：{', '.join(report['compareKeyColumns'])}",
        f"- 国家变化数：{report['countryScopeSummary']['changedCountryCount']}",
        f"- 新增月份：{', '.join(report['timeAxisCheck']['addedMonths']) or '无'}",
        f"- 缺失月份：{', '.join(report['timeAxisCheck']['removedMonths']) or '无'}",
        "",
        "## 2. 国家 freshness 结果",
        "",
        f"- advanced：{len(advanced)}",
        f"- unchanged_latest：{len(unchanged)}",
        (
            "- removedCountries："
            f"{len(report['countryScopeSummary']['removedCountries'])}"
        ),
        "",
        "## 3. 需要人工确认的国家",
        "",
    ]
    if review_findings:
        for finding in review_findings:
            lines.append(f"- {finding['target']} · {finding['message']}")
    else:
        lines.append("- 无")

    lines.extend([
        "",
        "## 4. blocker 列表",
        "",
    ])
    if blocker_findings:
        for finding in blocker_findings:
            lines.append(f"- {finding['target']} · {finding['message']}")
    else:
        lines.append("- 无")

    lines.extend([
        "",
        "## 5. promotion 建议",
        "",
        f"- 当前建议：{report['decisionSuggestion']}",
    ])
    return "\n".join(lines) + "\n"


def run_compare(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    job_id = args.job_id or build_job_id("raw-compare")
    logger = get_logger("jato.raw_compare", job_id=job_id)

    def emit(message: str) -> None:
        print(message)
        logger.info(message)

    old_file = resolve_explicit_file(args.old)
    new_file = resolve_explicit_file(args.new)
    emit(f"📥 读取 baseline: {to_project_relative(old_file)}")
    old_df = normalize_dataframe(
        read_excel_with_fallback(old_file, args.sheet)
    )
    emit(f"📥 读取 candidate: {to_project_relative(new_file)}")
    new_df = normalize_dataframe(
        read_excel_with_fallback(new_file, args.sheet)
    )

    old_columns = [str(column) for column in old_df.columns]
    new_columns = [str(column) for column in new_df.columns]
    country_candidates = tuple(
        dict.fromkeys((args.country_col, *DEFAULT_COUNTRY_CANDIDATES))
    )
    old_country_col = find_column_match(old_columns, country_candidates)
    new_country_col = find_column_match(new_columns, country_candidates)
    if not old_country_col or not new_country_col:
        raise ValueError("无法在两个输入文件中都解析国家列。")

    _, old_month_columns = detect_time_columns(old_columns)
    _, new_month_columns = detect_time_columns(new_columns)
    compare_plan = build_compare_plan(
        old_df=old_df,
        new_df=new_df,
        country_col_text=args.country_col,
        explicit_compare_keys=parse_csv_list(args.compare_keys),
    )

    old_summary = build_file_summary(
        old_file,
        old_df,
        old_country_col,
        old_month_columns,
        args.sheet,
    )
    new_summary = build_file_summary(
        new_file,
        new_df,
        new_country_col,
        new_month_columns,
        args.sheet,
    )
    compare_id = args.compare_id or build_compare_id(
        old_file=old_file,
        new_file=new_file,
        old_latest=old_summary["latestMonth"],
        new_latest=new_summary["latestMonth"],
    )
    output_dir = ensure_output_dir(args.output_dir, compare_id)

    old_country_info = collect_country_time_info(
        old_df,
        old_country_col,
        old_month_columns,
    )
    new_country_info = collect_country_time_info(
        new_df,
        new_country_col,
        new_month_columns,
    )
    freshness_entries = summarize_country_freshness(
        old_country_info,
        new_country_info,
        allow_missing_countries=bool(args.allow_missing_countries),
    )
    coverage_entries = summarize_country_coverage(
        old_country_info,
        new_country_info,
        allow_missing_countries=bool(args.allow_missing_countries),
    )
    emit(
        f"🔎 开始记录级重叠比对（{len(coverage_entries)} 个国家）"
    )
    overlap_summaries, conflict_samples = summarize_overlap_changes(
        old_df=old_df,
        new_df=new_df,
        compare_plan=compare_plan,
        old_country_col=old_country_col,
        new_country_col=new_country_col,
        coverage_entries=coverage_entries,
        old_time_columns=old_month_columns,
        new_time_columns=new_month_columns,
        sample_limit=max(int(args.sample_limit), 1),
        progress_callback=emit,
    )
    emit("✅ 完成记录级重叠比对")

    scope_summary = build_country_scope_summary(
        freshness_entries=freshness_entries,
        coverage_entries=coverage_entries,
        old_countries=collect_country_set(old_df, old_country_col),
        new_countries=collect_country_set(new_df, new_country_col),
        allow_missing_countries=bool(args.allow_missing_countries),
    )
    schema_check = build_schema_check(old_df, new_df, compare_plan)
    time_axis_check = build_time_axis_check(
        old_month_columns,
        new_month_columns,
    )
    review_findings = build_review_findings(
        schema_check=schema_check,
        time_axis_check=time_axis_check,
        compare_plan=compare_plan,
        scope_summary=scope_summary,
        freshness_entries=freshness_entries,
        coverage_entries=coverage_entries,
        overlap_summaries=overlap_summaries,
    )
    decision_suggestion = decide_review_outcome(review_findings)

    report = {
        "compareId": compare_id,
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "jobId": job_id,
        "baselineInput": old_summary,
        "candidateInput": new_summary,
        "compareKeyMode": compare_plan["compareKeyMode"],
        "compareKeyColumns": compare_plan["compareKeyColumns"],
        "allowMissingCountries": bool(args.allow_missing_countries),
        "outputDir": to_project_relative(output_dir),
        "schemaCheck": schema_check,
        "timeAxisCheck": time_axis_check,
        "countryScopeSummary": scope_summary,
        "countryFreshnessSummary": freshness_entries,
        "countryCoverageSummary": coverage_entries,
        "overlapChangeSummary": overlap_summaries,
        "reviewFindings": review_findings,
        "decisionSuggestion": decision_suggestion,
    }

    report_path = output_dir / "raw_compare_report.json"
    latest_month_csv_path = output_dir / "country_latest_month_diff.csv"
    coverage_csv_path = output_dir / "country_month_coverage_diff.csv"
    change_csv_path = output_dir / "country_change_summary.csv"
    conflict_sample_path = output_dir / "conflict_samples.json"
    checklist_path = output_dir / "review_checklist.md"

    write_json(report_path, report)
    write_csv(latest_month_csv_path, freshness_entries)
    write_csv(coverage_csv_path, coverage_entries)
    write_csv(
        change_csv_path,
        [
            {
                "country": summary["country"],
                "compareKeyColumns": json.dumps(
                    summary["compareKeyColumns"],
                    ensure_ascii=False,
                ),
                "addedRecordCount": summary["addedRecordCount"],
                "removedRecordCount": summary["removedRecordCount"],
                "changedRecordCount": summary["changedRecordCount"],
                "unchangedRecordCount": summary["unchangedRecordCount"],
                "changeRate": summary["changeRate"],
            }
            for summary in overlap_summaries
        ],
    )
    write_json(
        conflict_sample_path,
        {
            "compareId": compare_id,
            "compareKeyColumns": compare_plan["compareKeyColumns"],
            "sampledCountries": sorted(
                {sample["country"] for sample in conflict_samples}
            ),
            "samples": conflict_samples,
        },
    )
    checklist_path.write_text(
        build_review_checklist_markdown(report),
        encoding="utf-8",
    )

    emit(f"📄 报告: {to_project_relative(report_path)}")
    emit(f"📄 Checklist: {to_project_relative(checklist_path)}")
    emit(f"🧭 建议结论: {decision_suggestion}")

    exit_code = 0
    if decision_suggestion == "reject_input_batch":
        exit_code = 2
    elif args.strict and decision_suggestion != "proceed_to_candidate_refresh":
        exit_code = 2
    return report, exit_code


def build_compare_id(
    old_file: Path,
    new_file: Path,
    old_latest: str | None,
    new_latest: str | None,
) -> str:
    if old_latest and new_latest:
        return (
            f"{sanitize_identifier(old_latest)}"
            f"_vs_{sanitize_identifier(new_latest)}"
        )
    return (
        f"{sanitize_identifier(old_file.stem)}"
        f"_vs_{sanitize_identifier(new_file.stem)}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="对两个 raw Excel 做 compare / review，输出国家与月份差异报告。",
    )
    parser.add_argument(
        "--old",
        type=str,
        required=True,
        help="baseline Excel 路径。",
    )
    parser.add_argument(
        "--new",
        type=str,
        required=True,
        help="candidate Excel 路径。",
    )
    parser.add_argument(
        "--sheet",
        type=str,
        default=DEFAULT_SHEET,
        help="Excel Sheet 名称。",
    )
    parser.add_argument(
        "--country-col",
        type=str,
        default="国家",
        help="国家列名称，默认优先按 国家 解析。",
    )
    parser.add_argument(
        "--compare-keys",
        type=str,
        default=None,
        help="显式 compare keys（逗号分隔）；不传则自动识别。",
    )
    parser.add_argument(
        "--compare-id",
        type=str,
        default=None,
        help="compare 批次 ID；不传则根据输入自动生成。",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="输出目录；不传则写入 04_Processed_data/reviews/raw_compare/<compare_id>/。",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help="变更样本上限，默认 20。",
    )
    parser.add_argument(
        "--allow-missing-countries",
        action="store_true",
        help="允许候选文件只覆盖部分国家；未出现的 baseline 国家按沿用 baseline 处理。",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="manual review 或 blocker 时返回退出码 2。",
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
    try:
        _, exit_code = run_compare(args)
    except Exception as error:
        print(
            f"❌ Raw compare 失败[{type(error).__name__}] {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
