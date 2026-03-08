# pyright: reportMissingImports=false

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = PROJECT_ROOT / "03_Scripts"
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
for path in [SCRIPTS_ROOT, DASHBOARD_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from benchmark_dashboard_load import collect_benchmark  # noqa: E402
from benchmark_time_transform_pipeline import compare_mode  # noqa: E402
from dashboard.data import (  # noqa: E402
    get_dataset_version_token,
    load_column_names,
    load_dataset_slice,
    resolve_columns_from_names,
)
from dashboard.runner import (  # noqa: E402
    build_analysis_projection,
    resolve_data_source_path,
)
from dashboard.views import build_time_axis  # noqa: E402


DEFAULT_COUNTRY = "德国"


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def detect_dataset_availability() -> tuple[bool, str]:
    dataset_path = Path(resolve_data_source_path())
    if dataset_path.is_file():
        return dataset_path.exists(), str(dataset_path)

    if dataset_path.is_dir():
        has_parquet = any(dataset_path.rglob("*.parquet"))
        return has_parquet, str(dataset_path)

    return False, str(dataset_path)


def run_transform_reports(
    country: str,
    repeats: int,
) -> tuple[int, list[dict[str, float | int | str]]]:
    dataset_path = resolve_data_source_path()
    dataset_version = get_dataset_version_token(dataset_path)
    column_names = load_column_names(
        dataset_path,
        dataset_version=dataset_version,
    )
    columns = resolve_columns_from_names(column_names)
    analysis_projection = build_analysis_projection(column_names, columns)

    filter_payload: tuple[tuple[str, tuple[str, ...]], ...] = ()
    if country and columns.country:
        filter_payload = ((str(columns.country), (str(country),)),)

    filtered_df = load_dataset_slice(
        parquet_path=dataset_path,
        columns=analysis_projection or None,
        filter_payload=filter_payload,
        dataset_version=f"{dataset_version}:nightly:{country}",
        cache_scope="analysis",
    )
    time_axis = build_time_axis(filtered_df)
    if not time_axis:
        raise RuntimeError("No usable time axis columns detected.")

    selected_columns = list(time_axis.columns)
    reports: list[dict[str, float | int | str]] = []
    reports.append(
        compare_mode(
            filtered_df=filtered_df,
            selected_columns=selected_columns,
            grain=time_axis.grain,
            mode_name="sum",
            group_column=None,
            repeats=repeats,
        )
    )

    if columns.powertrain:
        reports.append(
            compare_mode(
                filtered_df=filtered_df,
                selected_columns=selected_columns,
                grain=time_axis.grain,
                mode_name="group:powertrain",
                group_column=str(columns.powertrain),
                repeats=repeats,
            )
        )

    return len(filtered_df), reports


def validate_transform_reports(
    scope: str,
    reports: list[dict[str, float | int | str]],
    min_speedup_sum: float,
    min_speedup_group: float,
    max_abs_diff: float,
) -> None:
    print(f"Transform gate: {scope}")
    for report in reports:
        mode = str(report["mode"])
        speedup = float(report["speedup"])
        max_diff = float(report["max_abs_diff"])
        p95_old = float(report["old_p95_seconds"])
        p95_new = float(report["new_p95_seconds"])
        print(
            f"- {mode}: speedup={speedup:.2f}x "
            f"p95_old={p95_old:.4f}s p95_new={p95_new:.4f}s "
            f"max_abs_diff={max_diff:.6f}"
        )

        target_speedup = min_speedup_sum
        if mode.startswith("group:"):
            target_speedup = min_speedup_group

        assert_true(
            speedup >= target_speedup,
            (
                f"{scope}/{mode} speedup too low: "
                f"actual={speedup:.2f}x expected>={target_speedup:.2f}x"
            ),
        )
        assert_true(
            max_diff <= max_abs_diff,
            (
                f"{scope}/{mode} parity mismatch: "
                f"actual={max_diff:.6f} allowed<={max_abs_diff:.6f}"
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Nightly performance gate for dashboard read/transform paths."
        ),
    )
    parser.add_argument(
        "--read-repeats",
        type=int,
        default=2,
        help="Repeats for read benchmark (default: 2).",
    )
    parser.add_argument(
        "--transform-repeats",
        type=int,
        default=2,
        help="Repeats for transform benchmark (default: 2).",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=DEFAULT_COUNTRY,
        help=f"Country scope for transform gate (default: {DEFAULT_COUNTRY}).",
    )
    parser.add_argument(
        "--min-read-speedup",
        type=float,
        default=2.0,
        help="Min projected-vs-full avg speedup (default: 2.0).",
    )
    parser.add_argument(
        "--min-transform-speedup-sum",
        type=float,
        default=20.0,
        help="Min transform speedup for sum mode (default: 20.0).",
    )
    parser.add_argument(
        "--min-transform-speedup-group",
        type=float,
        default=10.0,
        help="Min transform speedup for grouped mode (default: 10.0).",
    )
    parser.add_argument(
        "--max-abs-diff",
        type=float,
        default=1e-6,
        help="Max allowed parity diff (default: 1e-6).",
    )
    parser.add_argument(
        "--skip-if-missing-dataset",
        action="store_true",
        help=(
            "Skip nightly gate (exit 0) when dataset snapshot is missing."
        ),
    )
    args = parser.parse_args()

    print("Nightly performance gate")

    has_dataset, dataset_path = detect_dataset_availability()
    print(f"Dataset source: {dataset_path}")
    if not has_dataset:
        message = (
            "No parquet dataset detected for nightly gate. "
            f"Path checked: {dataset_path}"
        )
        if args.skip_if_missing_dataset:
            print(f"Nightly performance gate: SKIPPED ({message})")
            return
        raise FileNotFoundError(message)

    read_result = collect_benchmark(repeats=args.read_repeats)
    read_speedup = float(read_result["projectionSpeedupVsFull"])
    print(
        "Read gate: "
        f"projection_vs_full={read_speedup:.2f}x "
        f"(min {args.min_read_speedup:.2f}x)"
    )
    assert_true(
        read_speedup >= args.min_read_speedup,
        (
            "Read gate failed: projection speedup too low. "
            f"actual={read_speedup:.2f}x expected>={args.min_read_speedup:.2f}x"
        ),
    )

    full_rows, full_reports = run_transform_reports(
        country="",
        repeats=args.transform_repeats,
    )
    print(f"Transform scope full rows={full_rows:,}")
    validate_transform_reports(
        scope="full",
        reports=full_reports,
        min_speedup_sum=args.min_transform_speedup_sum,
        min_speedup_group=args.min_transform_speedup_group,
        max_abs_diff=args.max_abs_diff,
    )

    country_rows, country_reports = run_transform_reports(
        country=args.country,
        repeats=args.transform_repeats,
    )
    print(f"Transform scope country={args.country} rows={country_rows:,}")
    validate_transform_reports(
        scope=f"country:{args.country}",
        reports=country_reports,
        min_speedup_sum=args.min_transform_speedup_sum,
        min_speedup_group=args.min_transform_speedup_group,
        max_abs_diff=args.max_abs_diff,
    )

    print("Nightly performance gate: PASS")


if __name__ == "__main__":
    main()
