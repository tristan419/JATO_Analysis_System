"""Parse configuration field mapping Excel into a standard feature catalog.

Input: 配置字段映射表.xlsx (sheet: 配置字段映射表)
Output: list of feature catalog dicts with category, standard_field_name,
        feature_code, aliases, and display_order.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

_FIELD_MAPPING_SHEET = "配置字段映射表"
_HEADER_ROW = 5  # 0-indexed: row index 4
_DATA_START_ROW = 6  # 0-indexed: row index 5


def _normalize_category(raw: str) -> str:
    return raw.strip()


def _normalize_field_name(raw: str) -> str:
    return raw.strip()


def _build_feature_code(category: str, field_name: str) -> str:
    """Generate a stable feature_code from category + field_name.

    Example: ("基本信息", "品牌") -> "basic_info.brand"
    """
    cat_slug = _slugify(category)
    field_slug = _slugify(field_name)
    return f"{cat_slug}.{field_slug}"


def _slugify(text: str) -> str:
    """Convert Chinese/English text to a snake_case ASCII slug."""
    # Map known Chinese categories/fields to English slugs
    KNOWN: dict[str, str] = {
        "基本信息": "basic_info",
        "车身尺寸": "body_dimensions",
        "动力系统": "powertrain",
        "底盘转向": "chassis_steering",
        "安全配置": "safety",
        "辅助操控": "driver_assist",
        "外观配置": "exterior",
        "内饰配置": "interior",
        "舒适配置": "comfort",
        "科技配置": "technology",
        "品牌": "brand",
        "级别": "segment",
        "能源类型": "energy_type",
        "驱动形式": "drive_type",
        "车身结构": "body_structure",
        "长": "length_mm",
        "宽": "width_mm",
        "高": "height_mm",
        "轴距": "wheelbase_mm",
    }
    if text in KNOWN:
        return KNOWN[text]
    # Fallback: lowercase, replace non-alphanumeric with underscore
    slug = text.lower().strip()
    slug = re.sub(r"[^\w]+", "_", slug)
    slug = slug.strip("_")
    return slug or "unknown"


def parse_field_mapping(file_path: str | Path) -> dict[str, Any]:
    """Parse the field mapping Excel and return feature catalog records.

    Args:
        file_path: Path to 配置字段映射表.xlsx

    Returns:
        {
            "features": list of feature dicts,
            "total_count": int,
            "categories": list of unique category names,
            "warnings": list of warning strings,
        }
    """
    source_path = Path(file_path)
    df = pd.read_excel(source_path, sheet_name=_FIELD_MAPPING_SHEET, engine="calamine")

    features: list[dict[str, Any]] = []
    warnings: list[str] = []
    last_category: str | None = None
    seen_pairs: set[tuple[str, str]] = set()
    duplicate_pairs: set[tuple[str, str]] = set()

    for row_idx, (_idx, row) in enumerate(df.iterrows()):
        # Rows before data start
        excel_row = row_idx + 1  # 1-indexed for messages
        if excel_row < _DATA_START_ROW:
            continue

        seq_raw = row.iloc[1] if len(row) > 1 else None  # column B
        if seq_raw is None or (isinstance(seq_raw, float) and pd.isna(seq_raw)):
            # Stop when sequence numbers end (usage instructions area)
            if excel_row > 320:
                break
            continue

        try:
            seq = int(seq_raw)
        except (ValueError, TypeError):
            if excel_row > 320:
                break
            continue

        cat_raw = row.iloc[2] if len(row) > 2 else None  # column C
        field_raw = row.iloc[3] if len(row) > 3 else None  # column D

        if cat_raw and isinstance(cat_raw, str) and cat_raw.strip():
            last_category = _normalize_category(cat_raw)

        if not last_category:
            warnings.append(f"Row {excel_row}: no category available, skipping")
            continue

        if field_raw is None or (isinstance(field_raw, float) and pd.isna(field_raw)):
            continue

        field_name = _normalize_field_name(str(field_raw))
        if not field_name:
            continue

        pair = (last_category, field_name)
        if pair in seen_pairs:
            duplicate_pairs.add(pair)
        seen_pairs.add(pair)

        aliases: list[str] = []
        for col_idx in range(4, min(7, len(row))):  # columns E, F, G
            alias_val = row.iloc[col_idx] if col_idx < len(row) else None
            if alias_val and isinstance(alias_val, str) and alias_val.strip():
                aliases.append(alias_val.strip())

        feature_code = _build_feature_code(last_category, field_name)

        features.append(
            {
                "seq": seq,
                "category": last_category,
                "standard_field_name": field_name,
                "feature_code": feature_code,
                "aliases": aliases if aliases else None,
                "display_order": seq,
            }
        )

    for cat, field_name in duplicate_pairs:
        warnings.append(
            f"Duplicate feature: category='{cat}' field='{field_name}'"
        )

    categories = list(dict.fromkeys(f["category"] for f in features))

    return {
        "features": features,
        "total_count": len(features),
        "categories": categories,
        "warnings": warnings,
    }
