"""Parse Material Master XLSX files into structured SKU rows.

Expected format: multi-sheet workbook where each sheet represents a
model/powertrain combination with columns for brand, model, version,
colour, BOM template, and country-specific FOB values.
"""

from __future__ import annotations

import re
from pathlib import Path

import openpyxl


# Keywords that indicate a dual-tone / two-tone colour
DUAL_COLOUR_PATTERNS = [
    r"/",
    r"dual",
    r"two.tone",
    r"two tone",
    r"black roof",
    r"bi.colou?r",
    r"双色",
]


def _detect_colour_type(colour_name: str) -> str:
    """Return 'dual' if the colour name suggests dual-tone, else 'single'."""
    lower = colour_name.lower().strip()
    for pattern in DUAL_COLOUR_PATTERNS:
        if re.search(pattern, lower):
            return "dual"
    return "single"


def _fuzzy_match_header(candidates: list[str], targets: list[str]) -> dict[str, str]:
    """Map target field names to the best-matching header cell.

    Returns a dict like {"brand": "Brand", "model_name": "Model", ...}.
    """
    mapping: dict[str, str] = {}
    lower_map = {h.lower().strip(): h for h in candidates}

    for target in targets:
        target_lower = target.lower().replace("_", " ").strip()
        # exact match
        for lk, original in lower_map.items():
            if lk == target_lower:
                mapping[target] = original
                break
        else:
            # partial match
            for lk, original in lower_map.items():
                if target_lower in lk or lk in target_lower:
                    mapping[target] = original
                    break
    return mapping


def parse_material_master_xlsx(file_path: Path) -> dict:
    """Parse an OMODA & JAECOO Material Master XLSX file.

    Each sheet is treated as one model/powertrain group.
    Headers are auto-detected from the first row.

    Returns::

        {
            "rows": [ { row_index, sheet_name, brand, model_name, version,
                         exterior_color_name, exterior_color_code,
                         exterior_color_type, interior_color_name,
                         bom_template, material_code, base_fob_eur,
                         powertrain, warnings } ... ],
            "warnings": ["..."],
            "sheet_names": ["Sheet1", ...],
        }
    """
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    all_rows: list[dict] = []
    warnings: list[str] = []

    target_fields = [
        "brand",
        "model",
        "model_name",
        "version",
        "colour",
        "exterior_color",
        "exterior_color_code",
        "interior_color",
        "bom",
        "bom_template",
        "fob",
        "base_fob",
        "powertrain",
        "engine",
    ]

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows_iter = list(ws.iter_rows(min_row=1, max_row=min(ws.max_row or 0, 5000),
                                      values_only=False))
        if len(rows_iter) < 2:
            warnings.append(f"Sheet '{sheet_name}' has fewer than 2 rows — skipping")
            continue

        # Detect header row (first row with enough non-None values)
        header_cells: list[str] = []
        for row in rows_iter[:5]:
            vals = [str(c.value).strip() if c.value is not None else ""
                    for c in row]
            non_empty = [v for v in vals if v]
            if len(non_empty) >= 3:
                header_cells = vals
                data_start = row[0].row  # 1-indexed row number
                break
        else:
            warnings.append(
                f"Sheet '{sheet_name}' — could not detect header row"
            )
            continue

        mapping = _fuzzy_match_header(header_cells, target_fields)

        # Required field detection
        brand_col = mapping.get("brand")
        model_col = mapping.get("model") or mapping.get("model_name")
        version_col = mapping.get("version")
        colour_col = mapping.get("colour") or mapping.get("exterior_color")
        colour_code_col = mapping.get("exterior_color_code")
        interior_col = mapping.get("interior_color")
        bom_col = mapping.get("bom") or mapping.get("bom_template")
        fob_col = mapping.get("fob") or mapping.get("base_fob")
        powertrain_col = mapping.get("powertrain") or mapping.get("engine")

        if not model_col and not bom_col:
            warnings.append(
                f"Sheet '{sheet_name}' — missing model and BOM columns"
            )
            continue

        # Parse data rows
        for row in rows_iter[data_start:]:
            vals = {
                header_cells[i] if i < len(header_cells) else f"col_{i}":
                str(cell.value).strip() if cell.value is not None else ""
                for i, cell in enumerate(row)
            }

            # Skip completely empty rows
            non_empty_vals = [v for v in vals.values() if v]
            if len(non_empty_vals) < 2:
                continue

            raw_brand = vals.get(brand_col, "") if brand_col else ""
            raw_model = vals.get(model_col, "") if model_col else ""
            raw_version = vals.get(version_col, "") if version_col else ""
            raw_colour = vals.get(colour_col, "") if colour_col else ""
            raw_colour_code = (
                vals.get(colour_code_col, "") if colour_code_col else ""
            )
            raw_interior = vals.get(interior_col, "") if interior_col else ""
            raw_bom = vals.get(bom_col, "") if bom_col else ""
            raw_fob_str = vals.get(fob_col, "") if fob_col else ""
            raw_powertrain = (
                vals.get(powertrain_col, "") if powertrain_col else ""
            )

            row_warnings: list[str] = []

            # Detect colour type
            colour_type = _detect_colour_type(raw_colour)
            if colour_type == "dual" and not raw_colour:
                colour_type = "single"

            # Generate material code: replace ** in BOM with colour code
            material_code = raw_bom
            if raw_colour_code and "**" in raw_bom:
                material_code = raw_bom.replace("**", raw_colour_code)
            elif "**" in raw_bom:
                row_warnings.append(f"BOM has '**' but no colour code found; "
                                    f"material_code = BOM unchanged")

            if not material_code:
                row_warnings.append("Missing BOM — material_code is empty")

            # Parse FOB
            base_fob_eur: float | None = None
            if raw_fob_str:
                try:
                    base_fob_eur = float(
                        raw_fob_str.replace(",", "")
                        .replace("€", "")
                        .replace("EUR", "")
                        .strip()
                    )
                except ValueError:
                    row_warnings.append(
                        f"Could not parse FOB value: '{raw_fob_str}'"
                    )

            all_rows.append({
                "row_index": row[0].row,
                "sheet_name": sheet_name,
                "brand": raw_brand,
                "model_name": raw_model,
                "version": raw_version,
                "exterior_color_name": raw_colour,
                "exterior_color_code": raw_colour_code,
                "exterior_color_type": colour_type,
                "interior_color_name": raw_interior or None,
                "bom_template": raw_bom or None,
                "material_code": material_code,
                "base_fob_eur": base_fob_eur,
                "powertrain": raw_powertrain or None,
                "warnings": row_warnings,
            })

    wb.close()

    total_warnings = warnings + [
        f"Row {r['row_index']} ({r['sheet_name']}): {w}"
        for r in all_rows
        for w in r["warnings"]
    ]

    return {
        "rows": all_rows,
        "warnings": total_warnings,
        "sheet_names": wb.sheetnames,
    }
