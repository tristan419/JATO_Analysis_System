# pyright: reportMissingImports=false

import argparse
import statistics
import sys
import time
from typing import Any
from pathlib import Path

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


def timed_call(func, repeats: int) -> list[float]:
    values: list[float] = []
    for idx in range(repeats):
        started = time.perf_counter()
        func(idx)
        values.append(time.perf_counter() - started)
    return values


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "min": min(values),
        "avg": statistics.mean(values),
        "max": max(values),
    }


def format_seconds(seconds: float) -> str:
    return f"{seconds:.3f}s"


def collect_benchmark(repeats: int) -> dict[str, Any]:
    dataset_path = resolve_data_source_path()
    dataset_version = get_dataset_version_token(dataset_path)
    column_names = load_column_names(
        dataset_path,
        dataset_version=dataset_version,
    )
    columns = resolve_columns_from_names(column_names)

    filter_columns = tuple(
        column
        for column in [
            columns.country,
            columns.segment,
            columns.powertrain,
            columns.make,
            columns.model,
            columns.version,
        ]
        if column
    )
    analysis_projection = build_analysis_projection(column_names, columns)

    sidebar_times = timed_call(
        lambda idx: load_dataset_slice(
            parquet_path=dataset_path,
            columns=filter_columns or None,
            dataset_version=f"{dataset_version}:sidebar:{idx}",
            cache_scope="sidebar",
        ),
        repeats=repeats,
    )
    projected_times = timed_call(
        lambda idx: load_dataset_slice(
            parquet_path=dataset_path,
            columns=analysis_projection or None,
            dataset_version=f"{dataset_version}:projection:{idx}",
            cache_scope="analysis",
        ),
        repeats=repeats,
    )
    full_times = timed_call(
        lambda idx: load_dataset_slice(
            parquet_path=dataset_path,
            columns=None,
            dataset_version=f"{dataset_version}:full:{idx}",
            cache_scope="detail",
        ),
        repeats=repeats,
    )

    sidebar_stats = summarize(sidebar_times)
    projected_stats = summarize(projected_times)
    full_stats = summarize(full_times)

    speedup = (
        full_stats["avg"] / projected_stats["avg"]
        if projected_stats["avg"] > 0
        else 0
    )

    return {
        "datasetPath": dataset_path,
        "datasetVersion": dataset_version,
        "repeats": int(repeats),
        "filterColumnCount": int(len(filter_columns)),
        "projectionColumnCount": int(len(analysis_projection)),
        "sidebarStats": sidebar_stats,
        "projectedStats": projected_stats,
        "fullStats": full_stats,
        "projectionSpeedupVsFull": float(speedup),
    }


def print_benchmark_summary(result: dict[str, Any]) -> None:
    print("🚀 Dashboard 首屏读取性能回归")
    print(f"数据源: {result['datasetPath']}")
    print(f"版本令牌: {result['datasetVersion']}")
    print(f"重复次数: {result['repeats']}")
    print(f"筛选列数: {result['filterColumnCount']}")
    print(f"投影列数: {result['projectionColumnCount']}")

    sidebar_stats = result["sidebarStats"]
    projected_stats = result["projectedStats"]
    full_stats = result["fullStats"]

    print("\n📊 阶段耗时（秒）")
    print(
        "侧边栏读取: "
        f"min={format_seconds(sidebar_stats['min'])}, "
        f"avg={format_seconds(sidebar_stats['avg'])}, "
        f"max={format_seconds(sidebar_stats['max'])}"
    )
    print(
        "分析投影读取: "
        f"min={format_seconds(projected_stats['min'])}, "
        f"avg={format_seconds(projected_stats['avg'])}, "
        f"max={format_seconds(projected_stats['max'])}"
    )
    print(
        "分析全列读取: "
        f"min={format_seconds(full_stats['min'])}, "
        f"avg={format_seconds(full_stats['avg'])}, "
        f"max={format_seconds(full_stats['max'])}"
    )
    print(
        "\n⚡ 投影读取相对全列平均加速: "
        f"{result['projectionSpeedupVsFull']:.2f}x"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dashboard 首屏读取性能回归脚本。",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="每个阶段重复次数（默认 3）。",
    )
    args = parser.parse_args()

    result = collect_benchmark(args.repeats)
    print_benchmark_summary(result)


if __name__ == "__main__":
    main()
