# pyright: reportMissingImports=false

from datetime import date
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from dashboard.views import (  # noqa: E402
    resolve_calendar_indices,
    resolve_slider_indices,
)


def assert_equal(name: str, actual, expected) -> None:
    if actual != expected:
        raise AssertionError(
            f"{name} mismatch: expected={expected} actual={actual}"
        )


def main() -> None:
    print("Time selector consistency regression")

    labels = [
        "2024 Jan",
        "2024 Feb",
        "2024 Mar",
        "2024 Apr",
        "2024 May",
        "2024 Jun",
    ]
    dates = [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 3, 1),
        date(2024, 4, 1),
        date(2024, 5, 1),
        date(2024, 6, 1),
    ]

    for start_idx in range(len(labels)):
        for end_idx in range(start_idx, len(labels)):
            slider_indices = resolve_slider_indices(
                labels,
                labels[start_idx],
                labels[end_idx],
            )
            calendar_indices = resolve_calendar_indices(
                dates,
                dates[start_idx],
                dates[end_idx],
            )
            assert_equal(
                f"pair-{start_idx}-{end_idx}",
                slider_indices,
                calendar_indices,
            )

    reversed_slider = resolve_slider_indices(
        labels,
        labels[4],
        labels[2],
    )
    assert_equal(
        "reversed-slider",
        reversed_slider,
        [2, 3, 4],
    )

    reversed_calendar = resolve_calendar_indices(
        dates,
        dates[4],
        dates[2],
    )
    assert_equal(
        "reversed-calendar",
        reversed_calendar,
        [2, 3, 4],
    )

    out_of_range = resolve_calendar_indices(
        dates,
        date(2025, 1, 1),
        date(2025, 1, 1),
    )
    assert_equal(
        "out-of-range-nearest",
        out_of_range,
        [len(dates) - 1],
    )

    print("Time selector consistency: PASS")


if __name__ == "__main__":
    main()
