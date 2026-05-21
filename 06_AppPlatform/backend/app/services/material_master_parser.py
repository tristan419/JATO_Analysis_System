"""Parse Material Master XLSX files into structured SKU rows.

Expected format: multi-sheet workbook where each sheet is one model/powertrain.
Header row has: No., Code Name, Full Name, Configuration, BOM,
Exterior Color, Interior Color, then country FOB columns (e.g. "Croatia FOB").

Data uses carry-forward: model info appears on the first row of a group,
subsequent rows only fill colour + FOB values.
"""

from __future__ import annotations

import re
from pathlib import Path

import openpyxl


DUAL_COLOUR_PATTERNS = [
    r"/",
    r"dual",
    r"two.tone",
    r"two tone",
    r"black roof",
    r"bi.colou?r",
    r"双色",  # 双色
]


def _detect_colour_type(colour_name: str) -> str:
    lower = colour_name.lower().strip()
    for pattern in DUAL_COLOUR_PATTERNS:
        if re.search(pattern, lower):
            return "dual"
    return "single"


def _extract_colour_code(colour_name: str) -> str:
    """Extract colour code from a name like 'Phantom gray（GV）' or 'White (BW)'.

    Supports both Chinese （） and ASCII () brackets.
    """
    # Chinese brackets
    m = re.search(r"（([A-Za-z0-9]+)）", colour_name)
    if m:
        return m.group(1).upper()
    # ASCII brackets
    m = re.search(r"\(([A-Za-z0-9]+)\)", colour_name)
    if m:
        return m.group(1).upper()
    return ""


def _clean_colour_name(colour_name: str) -> str:
    """Remove colour code suffix from colour name."""
    name = re.sub(r"（[A-Za-z0-9]+）", "", colour_name)
    name = re.sub(r"\([A-Za-z0-9]+\)", "", name)
    return name.strip()


def _extract_bom_template(bom_raw: str) -> str | None:
    """Extract the BOM line containing '**' from potentially multi-line text.

    Also strips trailing Chinese/comments after the BOM code.
    """
    if not bom_raw:
        return None
    lines = bom_raw.strip().split("\n")
    for line in lines:
        line = line.strip()
        if "**" in line:
            # Strip trailing non-alphanumeric commentary
            # e.g. "T71607V**MM0006（总代法规升级）" -> "T71607V**MM0006"
            # Keep only the BOM pattern: alphanum + ** + alphanum
            m = re.match(r"([A-Za-z0-9]+\*\*[A-Za-z0-9]+)", line)
            if m:
                return m.group(1)
            return line
    for line in lines:
        line = line.strip()
        if line:
            return line
    return None


# Country alias mapping for normalisation
COUNTRY_ALIAS_MAP: dict[str, str] = {
    # Typo/alias → correct name
    "crotia": "Croatia",
    "hu": "Hungary",
    "波的": "Poland",
    "latvia": "Latvia",
    "austria": "Austria",
    "sweden": "Sweden",
    "greece": "Greece",
    "hungary": "Hungary",
    "croatia": "Croatia",
    # ISO codes → country names (for fallback mapping)
    "hr": "Croatia",
    "cz": "Czech Republic",
    "se": "Sweden",
    "ro": "Romania",
    "fi": "Finland",
    "gr": "Greece",
    "at": "Austria",
    "hu_iso": "Hungary",
    "lv": "Latvia",
    "pl": "Poland",
}


def _normalise_country_name(name: str) -> str:
    """Map country column names to standard names."""
    return COUNTRY_ALIAS_MAP.get(name.lower().strip(), name.strip())


def parse_material_master_xlsx(file_path: Path) -> dict:
    """Parse an OMODA & JAECOO Material Master XLSX file.

    Returns::
        {
            "rows": [ {...}, ... ],
            "warnings": [...],
            "sheet_names": [...],
        }
    """
    wb = openpyxl.load_workbook(file_path, data_only=True)
    all_rows: list[dict] = []
    warnings: list[str] = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        if ws.max_row is None or ws.max_row < 3:
            warnings.append(f"Sheet '{sheet_name}' has too few rows — skipping")
            continue

        # Read all rows
        raw_rows: list[list] = []
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, values_only=True):
            raw_rows.append(list(row))

        # Find header row — it has "No." or "BOM" or "Code Name"
        header_idx = -1
        for i, row in enumerate(raw_rows):
            first_cell = str(row[0]).strip() if row[0] is not None else ""
            rest = [str(c).strip().lower() if c is not None else "" for c in row[1:]]
            rest_text = " ".join(rest)
            if first_cell == "No." or "code name" in rest_text or "bom" in rest_text:
                header_idx = i
                break
        if header_idx < 0:
            warnings.append(f"Sheet '{sheet_name}' — could not find header row")
            continue

        headers: list[str] = [str(c).strip() if c else "" for c in raw_rows[header_idx]]

        # Identify column indices
        def col_idx(*names: str) -> int:
            for name in names:
                name_lower = name.lower()
                for j, h in enumerate(headers):
                    if h.lower() == name_lower or name_lower in h.lower():
                        return j
            return -1

        idx_no = col_idx("No.")
        idx_code_name = col_idx("Code Name")
        idx_full_name = col_idx("Full Name")
        idx_config = col_idx("Configuration")
        idx_bom = col_idx("BOM")
        idx_ext_colour = col_idx("Exterior Color")
        idx_int_colour = col_idx("Interior Color")

        # Country FOB columns: any header ending with "FOB" or containing " FOB"
        country_fob_cols: list[tuple[str, int]] = []
        for j, h in enumerate(headers):
            hl = h.lower().strip()
            if hl.endswith("fob") or " fob" in hl:
                country_name = re.sub(r"\s*fob\s*$", "", h, flags=re.IGNORECASE).strip()
                country_fob_cols.append((country_name, j))

        if idx_ext_colour < 0:
            warnings.append(
                f"Sheet '{sheet_name}' — no Exterior Color column found"
            )
            continue

        # Parse data rows with carry-forward for model fields
        current_model: dict[str, str] = {
            "code_name": "",
            "full_name": "",
            "configuration": "",
            "bom": "",
        }

        for i in range(header_idx + 1, len(raw_rows)):
            row = raw_rows[i]
            if len(row) <= idx_ext_colour:
                continue

            # Skip empty/separator rows
            non_empty = sum(
                1 for c in row if c is not None and str(c).strip()
            )
            if non_empty == 0:
                continue

            no_val = row[idx_no] if idx_no >= 0 and idx_no < len(row) else None
            code_val = (
                row[idx_code_name]
                if idx_code_name >= 0 and idx_code_name < len(row)
                else None
            )
            full_val = (
                row[idx_full_name]
                if idx_full_name >= 0 and idx_full_name < len(row)
                else None
            )
            config_val = (
                row[idx_config]
                if idx_config >= 0 and idx_config < len(row)
                else None
            )
            bom_val = (
                row[idx_bom]
                if idx_bom >= 0 and idx_bom < len(row)
                else None
            )

            no_str = str(no_val).strip() if no_val is not None else ""
            code_str = str(code_val).strip() if code_val is not None else ""
            full_str = str(full_val).strip() if full_val is not None else ""
            config_str = str(config_val).strip() if config_val is not None else ""
            bom_str = str(bom_val).strip() if bom_val is not None else ""

            # Update current model — only overwrite fields that have values
            if code_str:
                current_model["code_name"] = code_str
            if full_str:
                current_model["full_name"] = full_str
            if config_str:
                current_model["configuration"] = config_str
            if bom_str:
                current_model["bom"] = bom_str

            # Extract colour
            raw_colour = (
                str(row[idx_ext_colour]).strip()
                if row[idx_ext_colour] is not None
                else ""
            )
            if not raw_colour:
                continue  # rows without colour are skipped

            colour_code = _extract_colour_code(raw_colour)
            colour_name = _clean_colour_name(raw_colour)
            colour_type = _detect_colour_type(raw_colour)

            # Interior colour
            raw_interior = (
                str(row[idx_int_colour]).strip()
                if idx_int_colour >= 0 and row[idx_int_colour] is not None
                else ""
            )

            # BOM template
            bom_raw = current_model.get("bom", "")
            bom_template = _extract_bom_template(bom_raw) if bom_raw else None

            # Generate material code
            material_code = ""
            row_warnings: list[str] = []
            if bom_template and colour_code and "**" in bom_template:
                material_code = bom_template.replace("**", colour_code)
            elif bom_template and "**" in bom_template:
                row_warnings.append(f"No colour code for '{raw_colour}', BOM ** not replaced")
                material_code = bom_template
            elif bom_template:
                material_code = bom_template
                if "**" not in bom_template:
                    row_warnings.append("BOM has no '**' placeholder")

            if not material_code:
                row_warnings.append("Empty material_code")

            # Full name gives model/version/powertrain
            full_name = current_model.get("full_name", "")
            config = current_model.get("configuration", "")

            # Extract base FOB values per country
            base_fob_eur: float | None = None
            country_fobs: dict[str, float] = {}
            for country_name, cidx in country_fob_cols:
                val = row[cidx] if cidx < len(row) else None
                if val is not None:
                    try:
                        country_fobs[country_name] = float(
                            str(val).replace(",", "").replace("€", "").strip()
                        )
                    except (ValueError, TypeError):
                        pass

            # Use first available country FOB as base
            if country_fobs:
                base_fob_eur = list(country_fobs.values())[0]

            all_rows.append({
                "row_index": i + 1,
                "sheet_name": sheet_name,
                "brand": "",  # detected from model name
                "model_name": full_name or current_model.get("code_name", ""),
                "version": config or "",
                "exterior_color_name": colour_name,
                "exterior_color_code": colour_code,
                "exterior_color_type": colour_type,
                "interior_color_name": raw_interior or None,
                "bom_template": bom_template,
                "material_code": material_code,
                "base_fob_eur": base_fob_eur,
                "powertrain": None,  # from sheet name or code_name
                "country_fobs": country_fobs,
                "warnings": row_warnings,
            })

    wb.close()

    # Post-processing: detect brand from model name
    for row in all_rows:
        mn = row["model_name"].upper()
        if "JAECOO" in mn:
            row["brand"] = "JAECOO"
        elif "OMODA" in mn:
            row["brand"] = "OMODA"
        else:
            row["brand"] = ""

        # Extract powertrain from model name or sheet
        sn = row["sheet_name"].upper()
        for pt in ["BEV", "EV", "SHS", "HEV", "PHEV", "ICE"]:
            if pt in sn or pt in mn:
                row["powertrain"] = pt
                break

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
