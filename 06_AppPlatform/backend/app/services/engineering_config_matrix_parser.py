"""Parse vehicle engineering configuration matrix Excel.

Input: 在售可控资源表_模板.xlsx (sheet: 在售可控资源表)
Output: trims + feature values in normalized form.

The matrix has:
  - Rows = configuration features (308 features across 10 categories)
  - Columns (D onwards) = vehicle trims
  - Cells = configuration values (标配/选装/-/numeric values)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from app.services.config_availability import classify_availability

_MATRIX_SHEET = "在售可控资源表"
_TRIM_HEADER_ROW = 3  # 1-indexed Excel row
_HEADER_ROW = 4
_DATA_START_ROW = 5


def _forward_fill_categories(
    categories: list[str | None],
) -> list[str]:
    """Forward-fill None values in a category list."""
    result: list[str] = []
    last: str | None = None
    for cat in categories:
        if cat is not None:
            last = cat
        result.append(last if last is not None else "")
    return result


def _parse_trim_name(raw: str) -> dict[str, str]:
    """Attempt to parse a full trim name into brand/model/trim components.

    Examples:
        "JAECOO7-尊贵版" -> brand="JAECOO", model="JAECOO7", trim="尊贵版"
        "TIGGO7-尊贵版" -> brand="CHERY", model="TIGGO7", trim="尊贵版"

    The brand is inferred from known patterns; model_name and trim_name
    are split on the last hyphen.
    """
    BRAND_PREFIXES = {
        "JAECOO": "JAECOO",
        "OMODA": "OMODA",
        "TIGGO": "CHERY",
        "ARRIZO": "CHERY",
        "CHERY": "CHERY",
        "EXEED": "EXEED",
    }

    raw = raw.strip()
    brand = ""
    for prefix, brand_name in BRAND_PREFIXES.items():
        if raw.upper().startswith(prefix.upper()):
            brand = brand_name
            break

    if "-" in raw:
        parts = raw.rsplit("-", 1)
        model_name = parts[0].strip()
        trim_name = parts[1].strip()
    else:
        model_name = raw
        trim_name = raw

    return {
        "brand": brand,
        "model_name": model_name,
        "trim_name": trim_name,
        "full_trim_name": raw,
    }


def parse_config_matrix(
    file_path: str | Path,
    feature_catalog: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Parse the engineering config matrix Excel.

    Args:
        file_path: Path to 在售可控资源表_模板.xlsx
        feature_catalog: Optional pre-parsed feature catalog for matching.
                         If provided, features are matched by category+field_name.

    Returns:
        {
            "trims": list of trim dicts,
            "values": list of trim_feature_value dicts,
            "categories": list of category names,
            "summary": {"category_count": ..., "feature_count": ..., "trim_count": ..., "value_record_count": ...},
            "warnings": list of warning strings,
            "unmatched_features": list of (category, field_name) tuples,
        }
    """
    source_path = Path(file_path)

    # calamine is faster for xlsx; fall back to default engine
    try:
        df = pd.read_excel(
            source_path, sheet_name=_MATRIX_SHEET, engine="calamine", header=None
        )
    except Exception:
        df = pd.read_excel(source_path, sheet_name=_MATRIX_SHEET, header=None)

    warnings: list[str] = []

    # Row 3 (0-indexed: row 2): trim names from column D (index 3) onwards
    trim_names_row = df.iloc[2] if len(df) > 2 else None
    if trim_names_row is None:
        raise ValueError("Row 3 (trim names) not found in config matrix")

    trim_columns: list[tuple[int, str]] = []
    for col_idx in range(3, len(trim_names_row)):  # D column = index 3
        val = trim_names_row.iloc[col_idx]
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        trim_name = str(val).strip()
        if trim_name:
            trim_columns.append((col_idx, trim_name))

    if not trim_columns:
        raise ValueError("No trim columns found in config matrix (row 3, columns D+)")

    # Row 4 (0-indexed: row 3): header validation
    header_row = df.iloc[3] if len(df) > 3 else None
    if header_row is not None:
        col_b = str(header_row.iloc[1]).strip() if len(header_row) > 1 else ""
        col_c = str(header_row.iloc[2]).strip() if len(header_row) > 2 else ""
        if "配置大类" not in col_b and "配置" not in col_b:
            warnings.append(
                f"Unexpected header value in B{_HEADER_ROW}: '{col_b}', expected '配置大类'"
            )
        if "配置子项" not in col_c and "配置" not in col_c:
            warnings.append(
                f"Unexpected header value in C{_HEADER_ROW}: '{col_c}', expected '配置子项'"
            )

    # Build feature catalog lookup
    catalog_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if feature_catalog:
        for feat in feature_catalog:
            key = (feat["category"], feat["standard_field_name"])
            catalog_lookup[key] = feat

    # Parse data rows
    trims: list[dict[str, Any]] = []
    values: list[dict[str, Any]] = []
    seen_categories: list[str | None] = []
    unmatched_features: list[tuple[str, str]] = []
    duplicate_features: set[tuple[str, str]] = set()
    seen_feature_pairs: set[tuple[str, str, int]] = set()  # (cat, field, col)
    skipped_empty_feature = 0

    # Extract categories and feature names first for forward-fill
    raw_categories: list[str | None] = []
    raw_features: list[tuple[int, str | None, str | None]] = []  # (excel_row, cat, field)

    for row_idx in range(_DATA_START_ROW - 1, len(df)):
        if row_idx >= len(df):
            break
        row = df.iloc[row_idx]
        excel_row = row_idx + 1

        cat_raw = row.iloc[1] if len(row) > 1 else None  # column B
        feat_raw = row.iloc[2] if len(row) > 2 else None  # column C

        cat_val = str(cat_raw).strip() if cat_raw is not None and not (isinstance(cat_raw, float) and pd.isna(cat_raw)) else None
        feat_val = str(feat_raw).strip() if feat_raw is not None and not (isinstance(feat_raw, float) and pd.isna(feat_raw)) else None

        raw_categories.append(cat_val)
        raw_features.append((excel_row, cat_val, feat_val))

    filled_categories = _forward_fill_categories(raw_categories)

    # Parse trim data
    trim_info = [_parse_trim_name(name) for _, name in trim_columns]
    for col_idx, trim_name in trim_columns:
        tri = _parse_trim_name(trim_name)
        tri["source_column"] = col_idx
        trims.append(tri)

    # Parse values
    for i, (excel_row, _orig_cat, feat_val) in enumerate(raw_features):
        if not feat_val:
            skipped_empty_feature += 1
            continue

        category = filled_categories[i]
        if not category:
            warnings.append(f"Row {excel_row}: empty category after forward-fill, skipping")
            continue

        for col_idx, trim_name in trim_columns:
            row = df.iloc[excel_row - 1]
            cell_value = row.iloc[col_idx] if col_idx < len(row) else None

            if cell_value is None or (isinstance(cell_value, float) and pd.isna(cell_value)):
                raw_text = ""
            else:
                raw_text = str(cell_value).strip()

            availability, normalized_value, unit = classify_availability(
                raw_text if raw_text else None
            )

            # Match against catalog
            catalog_match = catalog_lookup.get((category, feat_val))
            feature_id_hint = catalog_match["feature_code"] if catalog_match else None

            if not catalog_match:
                unmatched_key = (category, feat_val)
                if unmatched_key not in unmatched_features:
                    unmatched_features.append(unmatched_key)

            # Detect duplicate features within same column
            pair_key = (category, feat_val, col_idx)
            if pair_key in seen_feature_pairs:
                duplicate_features.add((category, feat_val))
            seen_feature_pairs.add(pair_key)

            values.append(
                {
                    "category": category,
                    "feature_name": feat_val,
                    "feature_code": feature_id_hint,
                    "full_trim_name": trim_name,
                    "raw_value": raw_text,
                    "normalized_value": normalized_value,
                    "availability": availability,
                    "unit": unit,
                    "source_row": excel_row,
                    "source_column": chr(65 + col_idx),  # D, E, F, ...
                }
            )

    # Deduplicate categories list while preserving order
    unique_categories = list(dict.fromkeys(filled_categories))
    unique_categories = [c for c in unique_categories if c]

    for dup in duplicate_features:
        warnings.append(
            f"Duplicate feature under same category: category='{dup[0]}' field='{dup[1]}'"
        )

    if skipped_empty_feature:
        warnings.append(f"Skipped {skipped_empty_feature} rows with empty feature name")

    return {
        "trims": trims,
        "values": values,
        "categories": unique_categories,
        "summary": {
            "category_count": len(unique_categories),
            "feature_count": len(set((v["category"], v["feature_name"]) for v in values)),
            "trim_count": len(trim_columns),
            "value_record_count": len(values),
        },
        "warnings": warnings,
        "unmatched_features": unmatched_features,
    }
