"""Excel export for Order Genius — generates RO-June style workbooks.

One sheet per powertrain, columns: Model | Version | Colour |
Material Code | FOB(EUR) | Jan | Feb | ... | Dec | TTL
"""

from __future__ import annotations

import io

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
STRIPED_FILL = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
HISTORICAL_FONT = Font(name="Calibri", size=11, strikethrough=True, color="808080")
NORMAL_FONT = Font(name="Calibri", size=11)
THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)
CENTER_ALIGN = Alignment(horizontal="center", vertical="center")
LEFT_ALIGN = Alignment(horizontal="left", vertical="center")

HEADERS = [
    "Model", "Version", "Colour", "Material Code", "FOB(EUR)",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "TTL",
]


def _apply_cell_style(cell, font=None, fill=None, alignment=None, border=None):
    if font:
        cell.font = font
    if fill:
        cell.fill = fill
    if alignment:
        cell.alignment = alignment
    if border:
        cell.border = border


def generate_order_genius_excel(
    rows: list[dict],
    country_code: str,
    country_name: str,
    year: int,
    include_historical: bool = True,
) -> io.BytesIO:
    """Generate an Order Genius Excel workbook.

    Args:
        rows: Matrix rows from ``build_matrix()``.
        country_code: e.g. "RO"
        country_name: Display name for the country.
        year: Order year.
        include_historical: If True, include historical rows with quantity.

    Returns:
        A BytesIO buffer containing the .xlsx file.
    """
    wb = openpyxl.Workbook()
    # Keep one default sheet to avoid openpyxl "at least one sheet" error.
    # We'll remove it after adding real sheets, or rename it.

    # Group rows by powertrain
    groups: dict[str, list[dict]] = {}
    for row in rows:
        pt = row.get("powertrain") or "Other"
        groups.setdefault(pt, []).append(row)

    # Sort groups so "Other" is last
    sorted_keys = sorted(
        [k for k in groups if k != "Other"]
    ) + (["Other"] if "Other" in groups else [])

    if not sorted_keys:
        # Empty matrix — keep the default sheet with a placeholder
        ws = wb.active
        ws.title = "No Data"
        ws.cell(row=1, column=1, value=f"No order data for {country_name} ({country_code}) {year}")
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return output

    wb.remove(wb.active)  # remove default sheet

    for pt_key in sorted_keys:
        pt_rows = groups[pt_key]
        ws = wb.create_sheet(title=pt_key[:31])  # sheet name max 31 chars

        # Title row
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(HEADERS))
        title_cell = ws.cell(row=1, column=1,
                             value=f"Order Genius — {country_name} ({country_code}) {year}")
        title_cell.font = Font(name="Calibri", size=14, bold=True)

        # Header row
        for col_idx, header in enumerate(HEADERS, 1):
            cell = ws.cell(row=2, column=col_idx, value=header)
            _apply_cell_style(cell, font=HEADER_FONT, fill=HEADER_FILL,
                              alignment=CENTER_ALIGN, border=THIN_BORDER)

        # Data rows
        for row_idx, row in enumerate(pt_rows):
            excel_row = row_idx + 3
            is_historical = row.get("lifecycleStatus") == "historical"
            row_font = HISTORICAL_FONT if is_historical else NORMAL_FONT
            row_fill = STRIPED_FILL if row_idx % 2 == 1 else None

            # Fixed columns
            values = [
                row.get("modelName", ""),
                row.get("version", ""),
                row.get("colour", ""),
                row.get("materialCode", ""),
                row.get("fobEur"),
            ]

            for col_idx, val in enumerate(values, 1):
                cell = ws.cell(row=excel_row, column=col_idx, value=val)
                _apply_cell_style(cell, font=row_font, fill=row_fill,
                                  border=THIN_BORDER)

            # Month columns (6-16)
            months = row.get("months", {})
            for month_num in range(1, 13):
                col_idx = month_num + 5
                month_data = months.get(str(month_num), {})
                qty = month_data.get("quantity", 0) if month_data else 0
                cell = ws.cell(row=excel_row, column=col_idx, value=qty)
                _apply_cell_style(
                    cell, font=row_font, fill=row_fill,
                    alignment=CENTER_ALIGN, border=THIN_BORDER,
                )

            # TTL column (17)
            ttl_cell = ws.cell(row=excel_row, column=17, value=row.get("ttl", 0))
            _apply_cell_style(
                ttl_cell, font=row_font, fill=row_fill,
                alignment=CENTER_ALIGN, border=THIN_BORDER,
            )

        # Freeze header + left columns
        ws.freeze_panes = "F3"

        # Auto-fit column widths (approximate)
        for col_idx in range(1, len(HEADERS) + 1):
            max_width = len(str(HEADERS[col_idx - 1])) + 2
            for row_idx in range(3, len(pt_rows) + 3):
                val = ws.cell(row=row_idx, column=col_idx).value
                if val is not None:
                    max_width = max(max_width, len(str(val)) + 2)
            ws.column_dimensions[get_column_letter(col_idx)].width = min(
                max_width, 30
            )

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output
