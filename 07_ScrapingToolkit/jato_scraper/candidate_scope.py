"""Build MSRP scraping scope from rolling 12-month JATO sales rankings."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT / "04_Processed_data" / "msrp_candidate_scope" / "all_market"
)
DEFAULT_SOURCES_DIR = PROJECT_ROOT / "07_ScrapingToolkit" / "sources"
FALLBACK_DATASET_PATH = (
    PROJECT_ROOT / "04_Processed_data" / "jato_full_archive.parquet"
)
PARTITIONED_DATASET_PATH = (
    PROJECT_ROOT / "04_Processed_data" / "partitioned_dataset_v1"
)
MONTH_COL_PATTERN = re.compile(r"^\d{4} [A-Z][a-z]{2}$")
SOURCE_COVERAGE_LABELS = {
    "model_source": "model-scoped",
    "brand_source": "brand-scoped",
    "missing_source": "missing",
}


@dataclass(frozen=True)
class ScopeColumns:
    country: str
    brand: str
    model: str
    month_columns: tuple[str, ...]


@dataclass(frozen=True)
class FilterColumns:
    vehicle_category: str | None = None
    body_type: str | None = None
    segment: str | None = None


@dataclass(frozen=True)
class ScopeFilters:
    vehicle_category: str | None = None


@dataclass(frozen=True)
class CountryWindow:
    country: str
    latest_month: str
    window_start_month: str
    window_end_month: str
    window_columns: tuple[str, ...]
    months_in_window: int


@dataclass(frozen=True)
class SourceScope:
    source_code: str
    source_file: str
    country: str
    brand: str
    scope_kind: str
    jato_model: str | None = None


def _normalize_key(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split()).casefold()


def _coerce_path(path_value: str | Path | None) -> Path | None:
    if path_value is None:
        return None
    return Path(path_value).expanduser().resolve()


def resolve_dataset_path(
    dataset_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> Path:
    explicit_path = _coerce_path(dataset_path)
    if explicit_path is not None:
        if not explicit_path.exists():
            raise FileNotFoundError(
                f"Dataset path does not exist: {explicit_path}"
            )
        return explicit_path

    root = _coerce_path(project_root) or PROJECT_ROOT
    partitioned_path = root / "04_Processed_data" / "partitioned_dataset_v1"
    if partitioned_path.is_dir() and (
        (partitioned_path / "manifest.json").exists()
        or next(partitioned_path.rglob("*.parquet"), None) is not None
    ):
        return partitioned_path

    parquet_path = root / "04_Processed_data" / "jato_full_archive.parquet"
    if parquet_path.exists():
        return parquet_path

    raise FileNotFoundError(
        "Unable to locate JATO parquet dataset under 04_Processed_data."
    )


def _resolve_existing_column(
    column_names: list[str],
    candidates: tuple[str, ...],
) -> str | None:
    lookup = {_normalize_key(name): name for name in column_names}
    for candidate in candidates:
        match = lookup.get(_normalize_key(candidate))
        if match:
            return match
    return None


def get_month_columns_from_names(column_names: list[str]) -> list[str]:
    month_columns = [
        str(column).strip()
        for column in column_names
        if MONTH_COL_PATTERN.match(str(column).strip())
    ]

    def _parse_month(column_name: str) -> pd.Timestamp:
        return pd.to_datetime(column_name, format="%Y %b", errors="coerce")

    return sorted(
        month_columns,
        key=lambda column_name: (
            _parse_month(column_name).toordinal()
            if pd.notna(_parse_month(column_name))
            else float("inf"),
            column_name,
        ),
    )


def resolve_scope_columns(column_names: list[str]) -> ScopeColumns:
    country = _resolve_existing_column(
        column_names,
        ("国家", "Countries", "Country", "country"),
    )
    brand = _resolve_existing_column(
        column_names,
        ("Make", "品牌 (英)", "brand", "Brand"),
    )
    model = _resolve_existing_column(
        column_names,
        ("Model", "车型规整", "model"),
    )
    month_columns = tuple(get_month_columns_from_names(column_names))

    missing = [
        label
        for label, value in (
            ("country", country),
            ("brand", brand),
            ("model", model),
        )
        if value is None
    ]
    if missing:
        missing_text = ", ".join(missing)
        raise KeyError(f"Missing required sales scope columns: {missing_text}")
    if not month_columns:
        raise KeyError("No monthly sales columns were found in the dataset.")

    return ScopeColumns(
        country=country,
        brand=brand,
        model=model,
        month_columns=month_columns,
    )


def resolve_filter_columns(column_names: list[str]) -> FilterColumns:
    return FilterColumns(
        vehicle_category=_resolve_existing_column(
            column_names,
            ("车型类别", "Vehicle category", "vehicle_category"),
        ),
        body_type=_resolve_existing_column(
            column_names,
            ("Body type", "body_type", "body type"),
        ),
        segment=_resolve_existing_column(
            column_names,
            ("JATO global segment", "细分市场-欧", "segment"),
        ),
    )


def load_column_names(dataset_path: str | Path) -> list[str]:
    path = Path(dataset_path)
    if path.is_file():
        dataset = ds.dataset(path, format="parquet")
    else:
        dataset = ds.dataset(
            path,
            format="parquet",
            partitioning="hive",
            exclude_invalid_files=True,
        )
    return [str(name).strip() for name in dataset.schema.names]


def load_sales_frame(
    dataset_path: str | Path,
    columns: list[str],
) -> pd.DataFrame:
    path = Path(dataset_path)
    selected_columns = [str(column).strip() for column in columns]
    if path.is_file():
        return pd.read_parquet(path, columns=selected_columns)
    dataset = ds.dataset(
        path,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )
    return dataset.to_table(columns=selected_columns).to_pandas()


def prepare_sales_frame(
    frame: pd.DataFrame,
    scope_columns: ScopeColumns,
) -> pd.DataFrame:
    prepared = frame.copy()
    key_columns = (
        scope_columns.country,
        scope_columns.brand,
        scope_columns.model,
    )
    for column in key_columns:
        prepared[column] = (
            prepared[column]
            .astype("string")
            .fillna("")
            .str.strip()
        )
    prepared = prepared.loc[
        (prepared[scope_columns.country] != "")
        & (prepared[scope_columns.brand] != "")
        & (prepared[scope_columns.model] != "")
    ].copy()
    for month_column in scope_columns.month_columns:
        prepared[month_column] = pd.to_numeric(
            prepared[month_column],
            errors="coerce",
        ).fillna(0.0)
    return prepared


def _normalized_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.casefold()
    )


def apply_scope_filters(
    frame: pd.DataFrame,
    filter_columns: FilterColumns,
    filters: ScopeFilters | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if filters is None or not filters.vehicle_category:
        return frame, {}

    target_category = str(filters.vehicle_category).strip()
    normalized_target = _normalize_key(target_category)
    masks: list[pd.Series] = []
    applied_fields: list[str] = []

    if filter_columns.vehicle_category:
        masks.append(
            _normalized_series(frame[filter_columns.vehicle_category])
            == normalized_target
        )
        applied_fields.append(filter_columns.vehicle_category)

    if normalized_target == "suv":
        if filter_columns.body_type:
            masks.append(
                _normalized_series(
                    frame[filter_columns.body_type]
                ).str.contains(
                    "sport utility vehicle", regex=False
                )
            )
            applied_fields.append(filter_columns.body_type)
        if filter_columns.segment:
            masks.append(
                _normalized_series(frame[filter_columns.segment]).str.contains(
                    "suv",
                    regex=False,
                )
            )
            applied_fields.append(filter_columns.segment)

    if not masks:
        raise KeyError(
            "Vehicle category filtering was requested, but no compatible "
            "classification columns were found in the dataset."
        )

    combined_mask = masks[0]
    for mask in masks[1:]:
        combined_mask = combined_mask | mask

    filtered = frame.loc[combined_mask].copy()
    return filtered, {
        "vehicle_category": target_category,
        "applied_fields": list(dict.fromkeys(applied_fields)),
        "row_count_before": int(len(frame)),
        "row_count_after": int(len(filtered)),
    }


def build_country_windows(
    frame: pd.DataFrame,
    scope_columns: ScopeColumns,
) -> dict[str, CountryWindow]:
    month_columns = list(scope_columns.month_columns)
    country_totals = frame.groupby(scope_columns.country)[month_columns].sum()
    month_index = {column: index for index, column in enumerate(month_columns)}
    windows: dict[str, CountryWindow] = {}

    for country, row in country_totals.iterrows():
        latest_month = None
        for month_column in reversed(month_columns):
            if float(row.get(month_column, 0.0) or 0.0) > 0:
                latest_month = month_column
                break
        if latest_month is None:
            continue

        latest_index = month_index[latest_month]
        start_index = max(0, latest_index - 11)
        window_columns = tuple(month_columns[start_index: latest_index + 1])
        windows[str(country)] = CountryWindow(
            country=str(country),
            latest_month=latest_month,
            window_start_month=window_columns[0],
            window_end_month=window_columns[-1],
            window_columns=window_columns,
            months_in_window=len(window_columns),
        )

    return windows


def load_source_scopes(
    sources_dir: str | Path | None = None,
) -> list[SourceScope]:
    root = _coerce_path(sources_dir) or DEFAULT_SOURCES_DIR
    if not root.exists():
        return []

    def extract_scoped_models(profile: dict[str, Any]) -> list[str]:
        fixed_model = (
            profile.get("fixed_jato_model")
            or profile.get("fixed_model")
        )
        if fixed_model:
            return [str(fixed_model).strip()]

        models: list[str] = []
        model_rules = profile.get("model_rules")
        if isinstance(model_rules, list):
            for rule in model_rules:
                if not isinstance(rule, dict):
                    continue
                model_name = str(
                    rule.get("jato_model")
                    or rule.get("model")
                    or ""
                ).strip()
                if model_name and model_name not in models:
                    models.append(model_name)
        return models

    def append_source_scope(path: Path) -> None:
        with open(path, encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
        if not isinstance(payload, dict):
            return

        profile = payload.get("profile")
        if not isinstance(profile, dict):
            profile = {}

        scoped_models = extract_scoped_models(profile)
        if scoped_models:
            for model_name in scoped_models:
                source_scopes.append(
                    SourceScope(
                        source_code=str(
                            payload.get("source_code") or path.stem
                        ).strip(),
                        source_file=str(path.relative_to(root)),
                        country=str(payload.get("country") or "").strip(),
                        brand=str(payload.get("brand") or "").strip(),
                        scope_kind="model",
                        jato_model=model_name,
                    )
                )
            return

        source_scopes.append(
            SourceScope(
                source_code=str(
                    payload.get("source_code") or path.stem
                ).strip(),
                source_file=str(path.relative_to(root)),
                country=str(payload.get("country") or "").strip(),
                brand=str(payload.get("brand") or "").strip(),
                scope_kind="brand",
                jato_model=None,
            )
        )

    source_scopes: list[SourceScope] = []
    for path in sorted(root.rglob("*.yaml")):
        if path.name.startswith("_"):
            continue
        append_source_scope(path)

    for path in sorted(root.rglob("*.yml")):
        if path.name.startswith("_"):
            continue
        append_source_scope(path)

    return source_scopes


def find_candidate_coverage(
    country: str,
    brand: str,
    model: str,
    source_scopes: list[SourceScope],
) -> tuple[str, list[SourceScope]]:
    normalized_country = _normalize_key(country)
    normalized_brand = _normalize_key(brand)
    normalized_model = _normalize_key(model)

    brand_matches: list[SourceScope] = []
    model_matches: list[SourceScope] = []
    for source_scope in source_scopes:
        if _normalize_key(source_scope.country) != normalized_country:
            continue
        if _normalize_key(source_scope.brand) != normalized_brand:
            continue
        if source_scope.scope_kind == "model":
            if _normalize_key(source_scope.jato_model) == normalized_model:
                model_matches.append(source_scope)
            continue
        brand_matches.append(source_scope)

    if model_matches:
        return "model_source", sorted(
            model_matches,
            key=lambda item: item.source_file,
        )
    if brand_matches:
        return "brand_source", sorted(
            brand_matches,
            key=lambda item: item.source_file,
        )
    return "missing_source", []


def _format_source_scope(source_scope: SourceScope) -> str:
    suffix = f" [{source_scope.scope_kind}]"
    if source_scope.scope_kind == "model" and source_scope.jato_model:
        suffix = f" [{source_scope.scope_kind}:{source_scope.jato_model}]"
    return f"{source_scope.source_file} ({source_scope.source_code}){suffix}"


def build_country_candidates(
    frame: pd.DataFrame,
    scope_columns: ScopeColumns,
    country_window: CountryWindow,
    source_scopes: list[SourceScope],
    top_n: int,
) -> list[dict[str, Any]]:
    country_frame = frame.loc[
        frame[scope_columns.country] == country_window.country
    ].copy()
    country_frame["sales_12m"] = country_frame[
        list(country_window.window_columns)
    ].sum(axis=1)
    grouped = (
        country_frame.groupby(
            [scope_columns.brand, scope_columns.model],
            as_index=False,
        )["sales_12m"]
        .sum()
        .sort_values(
            by=["sales_12m", scope_columns.brand, scope_columns.model],
            ascending=[False, True, True],
            kind="stable",
        )
    )
    grouped = grouped.loc[grouped["sales_12m"] > 0].reset_index(drop=True)
    if top_n > 0:
        grouped = grouped.head(top_n).copy()

    candidates: list[dict[str, Any]] = []
    for rank, row in enumerate(grouped.to_dict("records"), start=1):
        brand = str(row[scope_columns.brand])
        model = str(row[scope_columns.model])
        sales_12m = float(row["sales_12m"])
        coverage_status, matched_sources = find_candidate_coverage(
            country=country_window.country,
            brand=brand,
            model=model,
            source_scopes=source_scopes,
        )
        candidates.append(
            {
                "country": country_window.country,
                "brand": brand,
                "model": model,
                "rank": rank,
                "sales_12m": round(sales_12m, 2),
                "latest_month": country_window.latest_month,
                "window_start_month": country_window.window_start_month,
                "window_end_month": country_window.window_end_month,
                "months_in_window": country_window.months_in_window,
                "coverage_status": coverage_status,
                "coverage_label": SOURCE_COVERAGE_LABELS[coverage_status],
                "coverage_sources": [
                    _format_source_scope(source_scope)
                    for source_scope in matched_sources
                ],
            }
        )

    return candidates


def build_candidate_scope_report_from_frame(
    frame: pd.DataFrame,
    scope_columns: ScopeColumns,
    source_scopes: list[SourceScope],
    top_n: int = 20,
    *,
    dataset_path: str | Path | None = None,
    filter_columns: FilterColumns | None = None,
    filters: ScopeFilters | None = None,
) -> dict[str, Any]:
    prepared_frame = prepare_sales_frame(frame, scope_columns)
    filtered_frame, filter_metadata = apply_scope_filters(
        prepared_frame,
        filter_columns or FilterColumns(),
        filters,
    )
    country_windows = build_country_windows(filtered_frame, scope_columns)

    coverage_summary = {
        "model_source": 0,
        "brand_source": 0,
        "missing_source": 0,
    }
    country_summaries: list[dict[str, Any]] = []
    all_candidates: list[dict[str, Any]] = []

    for country in sorted(country_windows):
        country_window = country_windows[country]
        candidates = build_country_candidates(
            frame=filtered_frame,
            scope_columns=scope_columns,
            country_window=country_window,
            source_scopes=source_scopes,
            top_n=top_n,
        )
        if not candidates:
            continue
        for candidate in candidates:
            coverage_summary[candidate["coverage_status"]] += 1
        country_summaries.append(
            {
                "country": country,
                "latest_month": country_window.latest_month,
                "window_start_month": country_window.window_start_month,
                "window_end_month": country_window.window_end_month,
                "months_in_window": country_window.months_in_window,
                "candidate_count": len(candidates),
                "missing_count": sum(
                    1
                    for candidate in candidates
                    if candidate["coverage_status"] == "missing_source"
                ),
                "candidates": candidates,
            }
        )
        all_candidates.extend(candidates)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path) if dataset_path else None,
        "top_n": top_n,
        "country_count": len(country_summaries),
        "candidate_count": len(all_candidates),
        "source_count": len(source_scopes),
        "filters": filter_metadata,
        "source_scope_summary": {
            "brand": sum(
                1
                for source_scope in source_scopes
                if source_scope.scope_kind == "brand"
            ),
            "model": sum(
                1
                for source_scope in source_scopes
                if source_scope.scope_kind == "model"
            ),
        },
        "coverage_summary": coverage_summary,
        "country_summaries": country_summaries,
        "all_candidates": all_candidates,
    }


def generate_candidate_scope_report(
    dataset_path: str | Path | None = None,
    sources_dir: str | Path | None = None,
    top_n: int = 20,
    project_root: str | Path | None = None,
    vehicle_category: str | None = None,
) -> dict[str, Any]:
    resolved_dataset_path = resolve_dataset_path(
        dataset_path=dataset_path,
        project_root=project_root,
    )
    column_names = load_column_names(resolved_dataset_path)
    scope_columns = resolve_scope_columns(column_names)
    filter_columns = resolve_filter_columns(column_names)
    source_scopes = load_source_scopes(sources_dir)
    selected_columns = [
        scope_columns.country,
        scope_columns.brand,
        scope_columns.model,
        *scope_columns.month_columns,
    ]
    for extra_column in (
        filter_columns.vehicle_category,
        filter_columns.body_type,
        filter_columns.segment,
    ):
        if extra_column and extra_column not in selected_columns:
            selected_columns.append(extra_column)
    frame = load_sales_frame(resolved_dataset_path, selected_columns)
    return build_candidate_scope_report_from_frame(
        frame=frame,
        scope_columns=scope_columns,
        source_scopes=source_scopes,
        top_n=top_n,
        dataset_path=resolved_dataset_path,
        filter_columns=filter_columns,
        filters=ScopeFilters(vehicle_category=vehicle_category),
    )


def render_candidate_scope_markdown(report: dict[str, Any]) -> str:
    coverage_summary = report["coverage_summary"]
    filters = report.get("filters") or {}
    lines = [
        "# MSRP Candidate Scope Report",
        "",
        f"Generated at UTC: {report['generated_at_utc']}",
        f"Dataset: {report['dataset_path']}",
        f"Top N per country: {report['top_n']}",
        "",
    ]

    if filters:
        lines.extend([
            "## Filters",
            "",
            f"- Vehicle category: {filters.get('vehicle_category')}",
            (
                "- Applied classification fields: "
                + ", ".join(filters.get("applied_fields") or [])
            ),
            (
                "- Rows kept after filtering: "
                f"{filters.get('row_count_after')} "
                f"/ {filters.get('row_count_before')}"
            ),
            "",
        ])

    lines.extend([
        "## Rule",
        "",
        (
            "- Latest month is determined per country from the most recent "
            "month with positive JATO sales."
        ),
        (
            "- The candidate window is the latest country month plus the "
            "prior 11 months."
        ),
        (
            "- Ranking is aggregated by country, brand, and model across "
            "all rows/trims."
        ),
        (
            "- Optional classification filters are applied before the "
            "12-month ranking is calculated."
        ),
        (
            "- Coverage labels: model-scoped = exact model YAML, "
            "brand-scoped = brand-wide YAML, missing = no YAML source yet."
        ),
        "",
        "## Coverage Summary",
        "",
        f"- Countries with candidates: {report['country_count']}",
        f"- Ranked candidates: {report['candidate_count']}",
        f"- Existing sources: {report['source_count']}",
        f"- Exact model coverage: {coverage_summary['model_source']}",
        f"- Brand-level coverage: {coverage_summary['brand_source']}",
        f"- Missing coverage: {coverage_summary['missing_source']}",
        "",
    ])

    for summary in report["country_summaries"]:
        lines.extend(
            [
                f"## {summary['country']}",
                "",
                (
                    f"Window: {summary['window_start_month']} -> "
                    f"{summary['window_end_month']} "
                    f"({summary['months_in_window']} months)"
                ),
                "",
                "| Rank | Brand | Model | Sales 12M | Coverage | Sources |",
                "| ---: | --- | --- | ---: | --- | --- |",
            ]
        )
        for candidate in summary["candidates"]:
            sources = (
                ", ".join(candidate["coverage_sources"])
                if candidate["coverage_sources"]
                else "-"
            )
            lines.append(
                (
                    "| {rank} | {brand} | {model} | {sales_12m:,.0f} | "
                    "{coverage_label} | {sources} |"
                ).format(
                    rank=candidate["rank"],
                    brand=candidate["brand"],
                    model=candidate["model"],
                    sales_12m=candidate["sales_12m"],
                    coverage_label=candidate["coverage_label"],
                    sources=sources,
                )
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_candidate_scope_report(
    report: dict[str, Any],
    output_dir: str | Path | None = None,
) -> dict[str, Path]:
    destination = _coerce_path(output_dir) or DEFAULT_OUTPUT_DIR
    destination.mkdir(parents=True, exist_ok=True)
    json_path = destination / "candidate_scope_report.json"
    markdown_path = destination / "candidate_scope_report.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_candidate_scope_markdown(report),
        encoding="utf-8",
    )
    return {
        "json": json_path,
        "markdown": markdown_path,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate country-level MSRP scrape scope from rolling "
            "12-month JATO sales."
        ),
    )
    parser.add_argument("--dataset-path", default=None)
    parser.add_argument("--sources-dir", default=str(DEFAULT_SOURCES_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument(
        "--vehicle-category",
        default=None,
        help="Optional vehicle category filter, for example SUV.",
    )
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    report = generate_candidate_scope_report(
        dataset_path=args.dataset_path,
        sources_dir=args.sources_dir,
        top_n=args.top_n,
        project_root=args.project_root,
        vehicle_category=args.vehicle_category,
    )
    output_paths: dict[str, Path] = {}
    if not args.no_write:
        output_paths = write_candidate_scope_report(report, args.output_dir)

    coverage_summary = report["coverage_summary"]
    print(
        (
            "Countries={countries} Candidates={candidates} Exact={exact} "
            "Brand={brand} Missing={missing}"
        ).format(
            countries=report["country_count"],
            candidates=report["candidate_count"],
            exact=coverage_summary["model_source"],
            brand=coverage_summary["brand_source"],
            missing=coverage_summary["missing_source"],
        )
    )
    if report.get("filters"):
        print(
            "FILTERS="
            + json.dumps(report["filters"], ensure_ascii=False, sort_keys=True)
        )
    for label, path in output_paths.items():
        print(f"{label.upper()}={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
