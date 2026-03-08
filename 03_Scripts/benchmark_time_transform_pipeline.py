# pyright: reportMissingImports=false

import argparse
import statistics
import sys
import time
from pathlib import Path
from typing import Callable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

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
from dashboard.views import (  # noqa: E402
    build_time_axis,
    build_time_long_dataframe,
    normalize_series,
    parse_time_keys,
)


def old_build_time_long_dataframe(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
    grain: str,
    group_column: str | None,
) -> pd.DataFrame:
    if not selected_columns:
        return pd.DataFrame(columns=["Series", "Date", "Sales"])

    if group_column:
        time_df = filtered_df[[group_column] + selected_columns].copy()
        time_df["Series"] = normalize_series(time_df[group_column])
    else:
        time_df = filtered_df[selected_columns].copy()
        time_df["Series"] = "\u603b\u548c"

    long_df = time_df.melt(
        id_vars=["Series"],
        value_vars=selected_columns,
        var_name="TimeKey",
        value_name="Sales",
    )
    long_df["Sales"] = pd.to_numeric(
        long_df["Sales"],
        errors="coerce",
    ).fillna(0.0)
    long_df["Date"] = parse_time_keys(long_df["TimeKey"], grain)
    long_df = long_df.dropna(subset=["Date"]).sort_values("Date")

    return long_df[["Series", "Date", "Sales"]]


def aggregate_sales(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["Series", "Date"], as_index=False)["Sales"]
        .sum()
        .sort_values(["Series", "Date"])
        .reset_index(drop=True)
    )


def timed_call(
    func: Callable[[], pd.DataFrame],
    repeats: int,
) -> tuple[list[float], pd.DataFrame]:
    values: list[float] = []
    final_df = pd.DataFrame()
    for _ in range(repeats):
        start = time.perf_counter()
        final_df = func()
        values.append(time.perf_counter() - start)
    return values, final_df


def summarize_seconds(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "min": 0.0,
            "avg": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "max": 0.0,
        }

    sorted_values = sorted(values)

    def percentile(p: float) -> float:
        if len(sorted_values) == 1:
            return float(sorted_values[0])
        rank = (len(sorted_values) - 1) * p
        low = int(rank)
        high = min(low + 1, len(sorted_values) - 1)
        fraction = rank - low
        return float(
            sorted_values[low] * (1.0 - fraction)
            + sorted_values[high] * fraction
        )

    return {
        "min": float(min(sorted_values)),
        "avg": float(statistics.mean(sorted_values)),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "max": float(max(sorted_values)),
    }


def compare_mode(
    filtered_df: pd.DataFrame,
    selected_columns: list[str],
    grain: str,
    mode_name: str,
    group_column: str | None,
    repeats: int,
) -> dict[str, float | int | str]:
    old_values, old_df = timed_call(
        lambda: old_build_time_long_dataframe(
            filtered_df,
            selected_columns,
            grain,
            group_column,
        ),
        repeats=repeats,
    )
    new_values, new_df = timed_call(
        lambda: build_time_long_dataframe(
            filtered_df,
            selected_columns,
            grain,
            group_column,
        ),
        repeats=repeats,
    )

    old_summary = summarize_seconds(old_values)
    new_summary = summarize_seconds(new_values)

    old_agg = aggregate_sales(old_df)
    new_agg = aggregate_sales(new_df)

    merged = old_agg.merge(
        new_agg,
        on=["Series", "Date"],
        how="outer",
        suffixes=("_old", "_new"),
    ).fillna(0.0)

    max_abs_diff = float(
        (merged["Sales_old"] - merged["Sales_new"]).abs().max()
    )
    speedup = (
        old_summary["avg"] / new_summary["avg"]
        if new_summary["avg"] > 0
        else 0.0
    )

    return {
        "mode": mode_name,
        "rows_old": int(len(old_df)),
        "rows_new": int(len(new_df)),
        "old_min_seconds": old_summary["min"],
        "old_avg_seconds": old_summary["avg"],
        "old_p50_seconds": old_summary["p50"],
        "old_p95_seconds": old_summary["p95"],
        "old_max_seconds": old_summary["max"],
        "new_min_seconds": new_summary["min"],
        "new_avg_seconds": new_summary["avg"],
        "new_p50_seconds": new_summary["p50"],
        "new_p95_seconds": new_summary["p95"],
        "new_max_seconds": new_summary["max"],
        "speedup": float(speedup),
        "max_abs_diff": max_abs_diff,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark old vs new time-transform pipeline and verify parity."
        ),
    )
    parser.add_argument(
        "--country",
        type=str,
        default="",
        help="Optional country filter value. Leave empty for full dataset.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of repeats for each benchmark mode.",
    )
    args = parser.parse_args()

    dataset_path = resolve_data_source_path()
    dataset_version = get_dataset_version_token(dataset_path)
    column_names = load_column_names(
        dataset_path,
        dataset_version=dataset_version,
    )
    columns = resolve_columns_from_names(column_names)

    analysis_projection = build_analysis_projection(column_names, columns)

    filter_payload: tuple[tuple[str, tuple[str, ...]], ...] = ()
    if args.country and columns.country:
        filter_payload = ((str(columns.country), (str(args.country),)),)

    filtered_df = load_dataset_slice(
        parquet_path=dataset_path,
        columns=analysis_projection or None,
        filter_payload=filter_payload,
        dataset_version=(
            f"{dataset_version}:transform-benchmark:{args.country}"
        ),
        cache_scope="analysis",
    )

    time_axis = build_time_axis(filtered_df)
    if not time_axis:
        raise RuntimeError("No usable time axis columns detected.")

    selected_columns = list(time_axis.columns)

    print("Time-transform pipeline benchmark")
    print(f"dataset: {dataset_path}")
    print(f"rows: {len(filtered_df):,}")
    print(f"time columns: {len(selected_columns)}")
    print(f"grain: {time_axis.grain}")
    print(f"country filter: {args.country or '(none)'}")
    print(f"repeats: {args.repeats}")

    reports: list[dict[str, float | int | str]] = []
    reports.append(
        compare_mode(
            filtered_df,
            selected_columns,
            time_axis.grain,
            mode_name="sum",
            group_column=None,
            repeats=args.repeats,
        )
    )

    if columns.powertrain:
        reports.append(
            compare_mode(
                filtered_df,
                selected_columns,
                time_axis.grain,
                mode_name="group:powertrain",
                group_column=str(columns.powertrain),
                repeats=args.repeats,
            )
        )

    print("\nResults")
    has_parity_issue = False
    for report in reports:
        print(
            f"- mode={report['mode']} "
            f"rows_old={report['rows_old']:,} "
            f"rows_new={report['rows_new']:,} "
            f"old[min/avg/p50/p95/max]={report['old_min_seconds']:.4f}/"
            f"{report['old_avg_seconds']:.4f}/"
            f"{report['old_p50_seconds']:.4f}/"
            f"{report['old_p95_seconds']:.4f}/"
            f"{report['old_max_seconds']:.4f}s "
            f"new[min/avg/p50/p95/max]={report['new_min_seconds']:.4f}/"
            f"{report['new_avg_seconds']:.4f}/"
            f"{report['new_p50_seconds']:.4f}/"
            f"{report['new_p95_seconds']:.4f}/"
            f"{report['new_max_seconds']:.4f}s "
            f"speedup={report['speedup']:.2f}x "
            f"max_abs_diff={report['max_abs_diff']:.6f}"
        )
        if float(report["max_abs_diff"]) > 1e-6:
            has_parity_issue = True

    if has_parity_issue:
        raise SystemExit(
            "Parity check failed: max_abs_diff exceeded tolerance."
        )

    print("\nParity check: PASS")


if __name__ == "__main__":
    main()
