"""Human-readable BOM Admin workbook export.

The workbook mirrors the external material-code reference layout: one model per
sheet, configuration/BOM groupings, colour rows, and country FOB columns.  It
also carries explicit machine-readable tier and price components so a later
preview import never has to infer pricing from colour names or cell fills.
"""

from __future__ import annotations

import io
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from app.infra.order_genius_repository import COUNTRY_NAMES_BY_CODE


BOM_ADMIN_EXPORT_SCHEMA_VERSION = "bom-admin-material-master-v1"

_HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
_HEADER_FONT = Font(name="Arial", size=10, bold=True, color="FFFFFF")
_SUBHEADER_FILL = PatternFill("solid", fgColor="D9EAF7")
_SURCHARGE_FILL = PatternFill("solid", fgColor="DCE6F1")
_GROUP_FILL = PatternFill("solid", fgColor="F3F6F9")
_CONFLICT_FILL = PatternFill("solid", fgColor="FDE9E7")
_THIN_GREY = Side(style="thin", color="B7C3D0")
_BORDER = Border(left=_THIN_GREY, right=_THIN_GREY, top=_THIN_GREY, bottom=_THIN_GREY)
_PRICE_FORMAT = '#,##0.00;[Red](#,##0.00);-'

_FIXED_HEADERS = (
    "No.",
    "Code Name",
    "Full Name",
    "Configuration",
    "BOM Template",
    "Material Code",
    "Exterior Color",
    "Colour Code",
    "Interior Color",
    "Colour Tier",
)


def _clean(value: object) -> str:
    return str(value or "").strip()


def _safe_sheet_name(raw_name: str, used_names: set[str]) -> str:
    base = re.sub(r"[\\/*?:\[\]]", " ", _clean(raw_name)) or "MODEL"
    base = re.sub(r"\s+", " ", base).strip()[:31]
    candidate = base
    suffix = 2
    while candidate.casefold() in used_names:
        marker = f" {suffix}"
        candidate = f"{base[: 31 - len(marker)]}{marker}"
        suffix += 1
    used_names.add(candidate.casefold())
    return candidate


def _normalise_countries(countries: Iterable[str], country_code: str | None) -> list[str]:
    available = sorted({_clean(code).upper() for code in countries if _clean(code)})
    if country_code:
        selected = _clean(country_code).upper()
        if selected not in available:
            raise ValueError(f"Country {selected} has no active BOM FOB data")
        return [selected]
    return available


def _group_sort_key(item: dict) -> tuple[str, str, str, str, str]:
    return (
        _clean(item.get("version")).casefold(),
        _clean(item.get("bomTemplate")).casefold(),
        _clean(item.get("interiorColorName")).casefold(),
        _clean(item.get("colourTier")).casefold(),
        _clean(item.get("colourCode")).casefold(),
    )


def _country_price_parts(item: dict, country_code: str) -> tuple[object, object, object, bool]:
    fob = (item.get("fobByCountry") or {}).get(country_code) or {}
    if fob.get("status") == "conflict":
        return "REVIEW", "REVIEW", "REVIEW", True

    final = fob.get("finalFobEur")
    base = fob.get("baseFobEur")
    tier = _clean(item.get("colourTier")).lower() or "single"
    if base is None and tier == "single":
        base = final

    surcharge = fob.get("colourSurchargeEur")
    if surcharge is None:
        if tier == "single":
            surcharge = 0
        elif base is not None and final is not None:
            surcharge = round(float(final) - float(base), 2)

    return base, surcharge, final, False


def _merge_group_columns(ws, start_row: int, end_row: int) -> None:
    if end_row <= start_row:
        return
    for column in (2, 3, 4, 5, 9):
        ws.merge_cells(
            start_row=start_row,
            start_column=column,
            end_row=end_row,
            end_column=column,
        )
        ws.cell(start_row, column).alignment = Alignment(
            horizontal="left",
            vertical="center",
            wrap_text=True,
        )


def _write_model_sheet(ws, items: list[dict], countries: list[str]) -> None:
    fixed_count = len(_FIXED_HEADERS)
    for column, label in enumerate(_FIXED_HEADERS, start=1):
        cell = ws.cell(1, column, label)
        ws.merge_cells(start_row=1, start_column=column, end_row=2, end_column=column)
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = _BORDER

    for country_index, country_code in enumerate(countries):
        start_col = fixed_count + country_index * 3 + 1
        country_name = COUNTRY_NAMES_BY_CODE.get(country_code, country_code)
        title = ws.cell(1, start_col, f"{country_name} ({country_code}) FOB")
        ws.merge_cells(start_row=1, start_column=start_col, end_row=1, end_column=start_col + 2)
        title.fill = _HEADER_FILL
        title.font = _HEADER_FONT
        title.alignment = Alignment(horizontal="center", vertical="center")
        for offset, label in enumerate(("Single Base", "Surcharge", "Final FOB")):
            cell = ws.cell(2, start_col + offset, label)
            cell.fill = _SUBHEADER_FILL
            cell.font = Font(name="Arial", size=9, bold=True, color="1F2937")
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = _BORDER

    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for item in sorted(items, key=_group_sort_key):
        grouped[
            (
                _clean(item.get("version")),
                _clean(item.get("bomTemplate")),
                _clean(item.get("interiorColorName")),
            )
        ].append(item)

    row = 3
    for (version, bom_template, interior), group_items in grouped.items():
        group_start = row
        for index, item in enumerate(group_items, start=1):
            tier = _clean(item.get("colourTier")).lower() or "single"
            exterior_name = _clean(item.get("colour"))
            colour_code = _clean(item.get("colourCode"))
            values = (
                index,
                _clean(item.get("modelCode")),
                _clean(item.get("modelName")),
                version,
                bom_template,
                _clean(item.get("materialCode")),
                f"{exterior_name} ({colour_code})" if colour_code else exterior_name,
                colour_code,
                interior,
                tier.title(),
            )
            for column, value in enumerate(values, start=1):
                cell = ws.cell(row, column, value)
                cell.font = Font(name="Arial", size=9)
                cell.alignment = Alignment(vertical="center", wrap_text=True)
                cell.border = _BORDER
                if tier in {"dual", "special"}:
                    cell.fill = _SURCHARGE_FILL

            for country_index, country_code in enumerate(countries):
                start_col = fixed_count + country_index * 3 + 1
                base, surcharge, final, conflict = _country_price_parts(item, country_code)
                for offset, value in enumerate((base, surcharge, final)):
                    cell = ws.cell(row, start_col + offset, value)
                    cell.font = Font(name="Arial", size=9)
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                    cell.border = _BORDER
                    if isinstance(value, (int, float)):
                        cell.number_format = _PRICE_FORMAT
                    if conflict:
                        cell.fill = _CONFLICT_FILL
                    elif tier in {"dual", "special"}:
                        cell.fill = _SURCHARGE_FILL
            row += 1

        _merge_group_columns(ws, group_start, row - 1)
        for column in (2, 3, 4, 5, 9):
            ws.cell(group_start, column).fill = _GROUP_FILL

    widths = (6, 16, 24, 24, 24, 24, 30, 12, 18, 12)
    for column, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(column)].width = width
    for column in range(fixed_count + 1, fixed_count + len(countries) * 3 + 1):
        ws.column_dimensions[get_column_letter(column)].width = 14

    ws.row_dimensions[1].height = 24
    ws.row_dimensions[2].height = 22
    ws.freeze_panes = "K3"
    ws.sheet_view.showGridLines = False
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.auto_filter.ref = f"A2:{get_column_letter(fixed_count + len(countries) * 3)}{max(row - 1, 2)}"


def _write_schema_sheet(wb, countries: list[str], item_count: int) -> None:
    ws = wb.create_sheet("_BOM_ADMIN_SCHEMA")
    rows = (
        ("schemaVersion", BOM_ADMIN_EXPORT_SCHEMA_VERSION),
        ("exportedAtUtc", datetime.now(timezone.utc).isoformat()),
        ("source", "BOM Admin / Material Master"),
        ("priceModel", "template-country Single base + saved colour tier surcharge"),
        ("tierAuthority", "BOM Admin saved Colour Tier; never inferred from name/code/swatch/fill"),
        ("countryScope", ",".join(countries)),
        ("itemCount", item_count),
    )
    for row_index, (key, value) in enumerate(rows, start=1):
        ws.cell(row_index, 1, key)
        ws.cell(row_index, 2, value)
    ws.sheet_state = "hidden"


def build_bom_admin_workbook(
    items: list[dict],
    countries: Iterable[str],
    *,
    country_code: str | None = None,
) -> io.BytesIO:
    """Build the BOM Admin material workbook for all countries or one country."""
    selected_countries = _normalise_countries(countries, country_code)
    if not selected_countries:
        raise ValueError("No active BOM FOB countries are available for export")

    model_groups: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        model_name = _clean(item.get("modelName")) or "UNNAMED MODEL"
        model_groups[model_name].append(item)
    if not model_groups:
        raise ValueError("No BOM Admin material rows are available for export")

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    used_names: set[str] = set()
    for model_name in sorted(model_groups, key=str.casefold):
        ws = wb.create_sheet(_safe_sheet_name(model_name, used_names))
        _write_model_sheet(ws, model_groups[model_name], selected_countries)
    _write_schema_sheet(wb, selected_countries, len(items))

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output
