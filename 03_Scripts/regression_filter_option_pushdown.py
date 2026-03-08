# pyright: reportMissingImports=false

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from dashboard.data import (  # noqa: E402
    build_filter_signature,
    get_dataset_version_token,
    load_column_names,
    load_dataset_slice,
    load_distinct_options,
    normalize_filter_payload,
    resolve_columns_from_names,
    unique_options,
)
from dashboard.runner import resolve_data_source_path  # noqa: E402


def assert_equal_options(
    name: str,
    expected: list[str],
    actual: list[str],
) -> None:
    print(
        f"{name}: expected={len(expected):,} actual={len(actual):,}"
    )
    if expected != actual:
        expected_only = sorted(set(expected) - set(actual))[:5]
        actual_only = sorted(set(actual) - set(expected))[:5]
        raise AssertionError(
            f"{name} mismatch: expected_only={expected_only} "
            f"actual_only={actual_only}"
        )


def build_payload(
    rules: list[tuple[str | None, list[str]]],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return normalize_filter_payload(
        [
            (column, values)
            for column, values in rules
            if column and values
        ]
    )


def load_pushdown_options(
    dataset_path: str,
    dataset_version: str,
    column: str,
    payload: tuple[tuple[str, tuple[str, ...]], ...],
) -> list[str]:
    return load_distinct_options(
        parquet_path=dataset_path,
        column=column,
        filter_payload=payload,
        dataset_version=dataset_version,
        filter_signature=build_filter_signature(payload),
    )


def load_expected_options(
    dataset_path: str,
    dataset_version: str,
    column: str,
    payload: tuple[tuple[str, tuple[str, ...]], ...],
) -> list[str]:
    frame = load_dataset_slice(
        parquet_path=dataset_path,
        columns=(column,),
        filter_payload=payload,
        dataset_version=dataset_version,
        filter_signature=build_filter_signature(payload),
        cache_scope="sidebar",
    )
    return unique_options(frame, column)


def main() -> None:
    print("Filter option pushdown regression")

    dataset_path = resolve_data_source_path()
    dataset_version = get_dataset_version_token(dataset_path)
    column_names = load_column_names(
        dataset_path,
        dataset_version=dataset_version,
    )
    columns = resolve_columns_from_names(column_names)

    required = [
        ("country", columns.country),
        ("segment", columns.segment),
        ("powertrain", columns.powertrain),
        ("make", columns.make),
        ("model", columns.model),
        ("version", columns.version),
    ]
    missing = [name for name, value in required if not value]
    if missing:
        raise SystemExit(f"缺少字段，无法回归: {', '.join(missing)}")

    assert columns.country
    assert columns.segment
    assert columns.powertrain
    assert columns.make
    assert columns.model
    assert columns.version

    no_filter_payload = build_payload([])
    signature_a = build_filter_signature(
        [(columns.country, ["德国", "法国"])]
    )
    signature_b = build_filter_signature(
        [(columns.country, ["法国", "德国"])]
    )
    if signature_a != signature_b:
        raise AssertionError("filter signature should be order-insensitive")

    country_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.country,
        no_filter_payload,
    )
    expected_countries = load_expected_options(
        dataset_path,
        dataset_version,
        columns.country,
        no_filter_payload,
    )
    assert_equal_options("country", expected_countries, country_options)

    selected_country = country_options[:1]
    country_payload = build_payload(
        [(columns.country, selected_country)]
    )
    segment_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.segment,
        country_payload,
    )
    expected_segments = load_expected_options(
        dataset_path,
        dataset_version,
        columns.segment,
        country_payload,
    )
    assert_equal_options("segment", expected_segments, segment_options)

    selected_segment = segment_options[:1]
    segment_payload = build_payload(
        [
            (columns.country, selected_country),
            (columns.segment, selected_segment),
        ]
    )
    powertrain_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.powertrain,
        segment_payload,
    )
    expected_powertrains = load_expected_options(
        dataset_path,
        dataset_version,
        columns.powertrain,
        segment_payload,
    )
    assert_equal_options(
        "powertrain",
        expected_powertrains,
        powertrain_options,
    )

    selected_powertrain = powertrain_options[:1]
    make_payload = build_payload(
        [
            (columns.country, selected_country),
            (columns.segment, selected_segment),
            (columns.powertrain, selected_powertrain),
        ]
    )
    make_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.make,
        make_payload,
    )
    expected_makes = load_expected_options(
        dataset_path,
        dataset_version,
        columns.make,
        make_payload,
    )
    assert_equal_options("make", expected_makes, make_options)

    selected_make = make_options[:1]
    model_payload = build_payload(
        [
            (columns.country, selected_country),
            (columns.segment, selected_segment),
            (columns.powertrain, selected_powertrain),
            (columns.make, selected_make),
        ]
    )
    model_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.model,
        model_payload,
    )
    expected_models = load_expected_options(
        dataset_path,
        dataset_version,
        columns.model,
        model_payload,
    )
    assert_equal_options("model", expected_models, model_options)

    selected_model = model_options[:1]
    version_payload = build_payload(
        [
            (columns.country, selected_country),
            (columns.segment, selected_segment),
            (columns.powertrain, selected_powertrain),
            (columns.make, selected_make),
            (columns.model, selected_model),
        ]
    )
    version_options = load_pushdown_options(
        dataset_path,
        dataset_version,
        columns.version,
        version_payload,
    )
    expected_versions = load_expected_options(
        dataset_path,
        dataset_version,
        columns.version,
        version_payload,
    )
    assert_equal_options("version", expected_versions, version_options)

    print("Filter option pushdown: PASS")


if __name__ == "__main__":
    main()
